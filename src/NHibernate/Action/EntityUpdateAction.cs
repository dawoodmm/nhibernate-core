#region

using System;
using System.Diagnostics;

using NHibernate.Cache;
using NHibernate.Cache.Access;
using NHibernate.Cache.Entry;
using NHibernate.Engine;
using NHibernate.Event;
using NHibernate.Persister.Entity;
using NHibernate.Type;

#endregion

namespace NHibernate.Action
{
    [Serializable]
    public sealed class EntityUpdateAction : EntityAction
    {
        #region Fields

        private readonly int[] dirtyFields;

        private readonly bool hasDirtyCollection;

        private readonly object[] previousState;

        private readonly object[] state;

        private object cacheEntry;

        private object nextVersion;

        private object previousVersion;

        private ISoftLock slock;

        #endregion

        #region Constructors and Destructors

        public EntityUpdateAction(
            object id,
            object[] state,
            int[] dirtyProperties,
            bool hasDirtyCollection,
            object[] previousState,
            object previousVersion,
            object nextVersion,
            object instance,
            IEntityPersister persister,
            ISessionImplementor session)
            : base(session, id, instance, persister)
        {
            this.state = state;
            this.previousState = previousState;
            this.previousVersion = previousVersion;
            this.nextVersion = nextVersion;
            this.dirtyFields = dirtyProperties;
            this.hasDirtyCollection = hasDirtyCollection;
        }

        #endregion

        #region Properties

        protected internal override bool HasPostCommitEventListeners
        {
            get { return this.Session.Listeners.PostCommitUpdateEventListeners.Length > 0; }
        }

        public bool HasPreviousUpdates { get; set; }
        #endregion

        #region Public Methods and Operators

        public override void Execute()
        {
            var session = this.Session;
            var id = this.Id;
            var persister = this.Persister;
            var instance = this.Instance;

            var statsEnabled = this.Session.Factory.Statistics.IsStatisticsEnabled;
            Stopwatch stopwatch = null;
            if (statsEnabled)
            {
                stopwatch = Stopwatch.StartNew();
            }

            var veto = this.PreUpdate();

            var factory = this.Session.Factory;

            if (persister.IsVersionPropertyGenerated || HasPreviousUpdates)
            {
                // we need to grab the version value from the entity, otherwise
                // we have issues with generated-version entities that may have
                // multiple actions queued during the same flush
                this.previousVersion = persister.GetVersion(instance, session.EntityMode);
            }

            CacheKey ck = null;
            if (persister.HasCache)
            {
                ck = session.GenerateCacheKey(id, persister.IdentifierType, persister.RootEntityName);
                this.slock = persister.Cache.Lock(ck, this.previousVersion);
            }

            if (!veto)
            {
                persister.Update(id, this.state, this.dirtyFields, this.hasDirtyCollection, this.previousState, this.previousVersion, instance, null, session);
            }

            var entry = this.Session.PersistenceContext.GetEntry(instance);
            if (entry == null)
            {
                throw new AssertionFailure("Possible nonthreadsafe access to session");
            }

            if (entry.Status == Status.Loaded || persister.IsVersionPropertyGenerated)
            {
                // get the updated snapshot of the entity state by cloning current state;
                // it is safe to copy in place, since by this time no-one else (should have)
                // has a reference  to the array
                TypeHelper.DeepCopy(this.state, persister.PropertyTypes, persister.PropertyCheckability, this.state, this.Session);
                if (persister.HasUpdateGeneratedProperties)
                {
                    // this entity defines property generation, so process those generated
                    // values...
                    persister.ProcessUpdateGeneratedProperties(id, instance, this.state, this.Session);
                    if (persister.IsVersionPropertyGenerated)
                    {
                        this.nextVersion = Versioning.GetVersion(this.state, persister);
                    }
                }
                // have the entity entry perform post-update processing, passing it the
                // update state and the new version (if one).
                entry.PostUpdate(instance, this.state, this.nextVersion);
            }

            if (persister.HasCache)
            {
                if (persister.IsCacheInvalidationRequired || entry.Status != Status.Loaded)
                {
                    persister.Cache.Evict(ck);
                }
                else
                {
                    var ce = new CacheEntry(this.state, persister, persister.HasUninitializedLazyProperties(instance, session.EntityMode), this.nextVersion, this.Session, instance);
                    this.cacheEntry = persister.CacheEntryStructure.Structure(ce);

                    var put = persister.Cache.Update(ck, this.cacheEntry, this.nextVersion, this.previousVersion);

                    if (put && factory.Statistics.IsStatisticsEnabled)
                    {
                        factory.StatisticsImplementor.SecondLevelCachePut(this.Persister.Cache.RegionName);
                    }
                }
            }

            this.PostUpdate();

            if (statsEnabled && !veto)
            {
                stopwatch.Stop();
                factory.StatisticsImplementor.UpdateEntity(this.Persister.EntityName, stopwatch.Elapsed);
            }
        }

        #endregion

        #region Methods

        protected override void AfterTransactionCompletionProcessImpl(bool success)
        {
            var persister = this.Persister;
            if (persister.HasCache)
            {
                var ck = this.Session.GenerateCacheKey(this.Id, persister.IdentifierType, persister.RootEntityName);

                if (success && this.cacheEntry != null)
                {
                    var put = persister.Cache.AfterUpdate(ck, this.cacheEntry, this.nextVersion, this.slock);

                    if (put && this.Session.Factory.Statistics.IsStatisticsEnabled)
                    {
                        this.Session.Factory.StatisticsImplementor.SecondLevelCachePut(this.Persister.Cache.RegionName);
                    }
                }
                else
                {
                    persister.Cache.Release(ck, this.slock);
                }
            }
            if (success)
            {
                this.PostCommitUpdate();
            }
        }

        private void PostCommitUpdate()
        {
            var postListeners = this.Session.Listeners.PostCommitUpdateEventListeners;
            if (postListeners.Length > 0)
            {
                var postEvent = new PostUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in postListeners)
                {
                    listener.OnPostUpdate(postEvent);
                }
            }
        }

        private void PostUpdate()
        {
            var postListeners = this.Session.Listeners.PostUpdateEventListeners;
            if (postListeners.Length > 0)
            {
                var postEvent = new PostUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in postListeners)
                {
                    listener.OnPostUpdate(postEvent);
                }
            }
        }

        private bool PreUpdate()
        {
            var preListeners = this.Session.Listeners.PreUpdateEventListeners;
            var veto = false;
            if (preListeners.Length > 0)
            {
                var preEvent = new PreUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in preListeners)
                {
                    veto |= listener.OnPreUpdate(preEvent);
                }
            }
            return veto;
        }

        #endregion
    }

    [Serializable]
    public sealed class EntityPreDeleteUpdateAction : EntityAction
    {
        #region Fields

        private readonly int[] dirtyFields;

        private readonly bool hasDirtyCollection;

        private readonly object[] previousState;

        private readonly object[] state;

        private object cacheEntry;

        private object nextVersion;

        private object previousVersion;

        private ISoftLock slock;

        #endregion

        #region Constructors and Destructors

        public EntityPreDeleteUpdateAction(
            object id,
            object[] state,
            int[] dirtyProperties,
            bool hasDirtyCollection,
            object[] previousState,
            object previousVersion,
            object nextVersion,
            object instance,
            IEntityPersister persister,
            ISessionImplementor session)
            : base(session, id, instance, persister)
        {
            this.state = state;
            this.previousState = previousState;
            this.previousVersion = previousVersion;
            this.nextVersion = nextVersion;
            this.dirtyFields = dirtyProperties;
            this.hasDirtyCollection = hasDirtyCollection;
        }

        #endregion

        #region Properties

        protected internal override bool HasPostCommitEventListeners
        {
            get { return this.Session.Listeners.PostCommitUpdateEventListeners.Length > 0; }
        }

        #endregion

        #region Public Methods and Operators

        public override void Execute()
        {
            var session = this.Session;
            var id = this.Id;
            var persister = this.Persister;
            var instance = this.Instance;

            var statsEnabled = this.Session.Factory.Statistics.IsStatisticsEnabled;
            Stopwatch stopwatch = null;
            if (statsEnabled)
            {
                stopwatch = Stopwatch.StartNew();
            }

            var veto = this.PreUpdate();

            var factory = this.Session.Factory;

            if (persister.IsVersionPropertyGenerated)
            {
                // we need to grab the version value from the entity, otherwise
                // we have issues with generated-version entities that may have
                // multiple actions queued during the same flush
                this.previousVersion = persister.GetVersion(instance, session.EntityMode);
            }

            CacheKey ck = null;
            if (persister.HasCache)
            {
                ck = session.GenerateCacheKey(id, persister.IdentifierType, persister.RootEntityName);
                this.slock = persister.Cache.Lock(ck, this.previousVersion);
            }

            if (!veto)
            {
                persister.Update(id, this.state, this.dirtyFields, this.hasDirtyCollection, this.previousState, this.previousVersion, instance, null, session);
            }

            var entry = this.Session.PersistenceContext.GetEntry(instance);
            if (entry == null)
            {
                throw new AssertionFailure("Possible nonthreadsafe access to session");
            }

            if (entry.Status == Status.Loaded || persister.IsVersionPropertyGenerated)
            {
                // get the updated snapshot of the entity state by cloning current state;
                // it is safe to copy in place, since by this time no-one else (should have)
                // has a reference  to the array
                TypeHelper.DeepCopy(this.state, persister.PropertyTypes, persister.PropertyCheckability, this.state, this.Session);
                if (persister.HasUpdateGeneratedProperties)
                {
                    // this entity defines property generation, so process those generated
                    // values...
                    persister.ProcessUpdateGeneratedProperties(id, instance, this.state, this.Session);
                    if (persister.IsVersionPropertyGenerated)
                    {
                        this.nextVersion = Versioning.GetVersion(this.state, persister);
                    }
                }
                // have the entity entry perform post-update processing, passing it the
                // update state and the new version (if one).
                entry.PostUpdate(instance, this.state, this.nextVersion);
            }

            if (persister.HasCache)
            {
                if (persister.IsCacheInvalidationRequired || entry.Status != Status.Loaded)
                {
                    persister.Cache.Evict(ck);
                }
                else
                {
                    var ce = new CacheEntry(this.state, persister, persister.HasUninitializedLazyProperties(instance, session.EntityMode), this.nextVersion, this.Session, instance);
                    this.cacheEntry = persister.CacheEntryStructure.Structure(ce);

                    var put = persister.Cache.Update(ck, this.cacheEntry, this.nextVersion, this.previousVersion);

                    if (put && factory.Statistics.IsStatisticsEnabled)
                    {
                        factory.StatisticsImplementor.SecondLevelCachePut(this.Persister.Cache.RegionName);
                    }
                }
            }

            this.PostUpdate();

            if (statsEnabled && !veto)
            {
                stopwatch.Stop();
                factory.StatisticsImplementor.UpdateEntity(this.Persister.EntityName, stopwatch.Elapsed);
            }
        }

        #endregion

        #region Methods

        protected override void AfterTransactionCompletionProcessImpl(bool success)
        {
            var persister = this.Persister;
            if (persister.HasCache)
            {
                var ck = this.Session.GenerateCacheKey(this.Id, persister.IdentifierType, persister.RootEntityName);

                if (success && this.cacheEntry != null)
                {
                    var put = persister.Cache.AfterUpdate(ck, this.cacheEntry, this.nextVersion, this.slock);

                    if (put && this.Session.Factory.Statistics.IsStatisticsEnabled)
                    {
                        this.Session.Factory.StatisticsImplementor.SecondLevelCachePut(this.Persister.Cache.RegionName);
                    }
                }
                else
                {
                    persister.Cache.Release(ck, this.slock);
                }
            }
            if (success)
            {
                this.PostCommitUpdate();
            }
        }

        private void PostCommitUpdate()
        {
            var postListeners = this.Session.Listeners.PostCommitUpdateEventListeners;
            if (postListeners.Length > 0)
            {
                var postEvent = new PostUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in postListeners)
                {
                    listener.OnPostUpdate(postEvent);
                }
            }
        }

        private void PostUpdate()
        {
            var postListeners = this.Session.Listeners.PostUpdateEventListeners;
            if (postListeners.Length > 0)
            {
                var postEvent = new PostUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in postListeners)
                {
                    listener.OnPostUpdate(postEvent);
                }
            }
        }

        private bool PreUpdate()
        {
            var preListeners = this.Session.Listeners.PreUpdateEventListeners;
            var veto = false;
            if (preListeners.Length > 0)
            {
                var preEvent = new PreUpdateEvent(this.Instance, this.Id, this.state, this.previousState, this.Persister, (IEventSource)this.Session);
                foreach (var listener in preListeners)
                {
                    veto |= listener.OnPreUpdate(preEvent);
                }
            }
            return veto;
        }

        #endregion
    }
}