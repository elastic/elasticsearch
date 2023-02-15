/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.allocation.StatelessAllocationDecider;
import co.elastic.elasticsearch.stateless.allocation.StatelessShardRoutingRoleStrategy;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.engine.TranslogReplicator;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class Stateless extends Plugin implements EnginePlugin, ActionPlugin, ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(Stateless.class);

    public static final String NAME = "stateless";

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    public static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    private final SetOnce<ObjectStoreService> objectStoreService = new SetOnce<>();

    private final SetOnce<SharedBlobCacheService<FileCacheKey>> sharedBlobCacheService = new SetOnce<>();

    private final SetOnce<TranslogReplicator> translogReplicator = new SetOnce<>();

    private final Settings settings;

    private ObjectStoreService getObjectStoreService() {
        return Objects.requireNonNull(this.objectStoreService.get());
    }

    public Stateless(Settings settings) {
        this.settings = requireValidSettings(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(TransportNewCommitNotificationAction.TYPE, TransportNewCommitNotificationAction.class));
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService
    ) {
        var objectStoreService = new ObjectStoreService(settings, repositoriesServiceSupplier, threadPool, clusterService, client);
        this.objectStoreService.set(objectStoreService);
        var sharedBlobCache = new SharedBlobCacheService<FileCacheKey>(nodeEnvironment, settings, threadPool);
        this.sharedBlobCacheService.set(sharedBlobCache);
        TranslogReplicator translogReplicator = new TranslogReplicator(threadPool, settings, objectStoreService);
        this.translogReplicator.set(translogReplicator);
        return List.of(objectStoreService, translogReplicator, sharedBlobCache);
    }

    @Override
    public void close() throws IOException {
        Releasables.close(sharedBlobCacheService.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            STATELESS_ENABLED,
            ObjectStoreService.TYPE_SETTING,
            ObjectStoreService.BUCKET_SETTING,
            ObjectStoreService.CLIENT_SETTING,
            ObjectStoreService.BASE_PATH_SETTING,
            IndexEngine.INDEX_FLUSH_INTERVAL_SETTING,
            ObjectStoreService.OBJECT_STORE_SHUTDOWN_TIMEOUT,
            TranslogReplicator.FLUSH_INTERVAL_SETTING
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE)) {
            indexModule.setIndexCommitListener(createIndexCommitListener());
        }
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)) {
            indexModule.setDirectoryWrapper((in, shardRouting) -> {
                if (shardRouting.isSearchable()) {
                    in.close();
                    return new SearchDirectory(sharedBlobCacheService.get(), shardRouting.shardId());
                } else {
                    return in;
                }
            });
        }
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings, ActionListener<Void> listener) {
                ActionListener.completeWith(listener, () -> {
                    if (indexShard.routingEntry().role().isSearchable()) {
                        final BlobContainer blobContainer = objectStoreService.get()
                            .getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());

                        final Store store = indexShard.store();
                        store.incRef();
                        try {
                            var searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
                            searchDirectory.setBlobContainer(blobContainer);
                            final Map<String, StoreFileMetadata> commit = ObjectStoreService.findSearchShardFiles(blobContainer);
                            logger.debug(() -> {
                                var segments = commit.keySet().stream().filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).findFirst();
                                var shardId = indexShard.shardId();
                                if (segments.isPresent()) {
                                    return format("[%s] bootstrapping shard from object store using commit [%s]", shardId, segments.get());
                                } else {
                                    return format("[%s] bootstrapping shard from object store using empty commit", shardId);
                                }
                            });
                            searchDirectory.updateCommit(commit);
                        } finally {
                            store.decRef();
                        }
                    }
                    return null;
                });
            }
        });
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.of(config -> {
            TranslogReplicator replicator = translogReplicator.get();
            TranslogConfig translogConfig = config.getTranslogConfig();
            replicator.setBigArrays(translogConfig.getBigArrays());
            if (config.isPromotableToPrimary()) {
                TranslogConfig newTranslogConfig = new TranslogConfig(
                    translogConfig.getShardId(),
                    translogConfig.getTranslogPath(),
                    translogConfig.getIndexSettings(),
                    translogConfig.getBigArrays(),
                    translogConfig.getBufferSize(),
                    translogConfig.getDiskIoBufferPool(),
                    (data, seqNo, location) -> replicator.add(translogConfig.getShardId(), data, seqNo, location)
                );
                EngineConfig newConfig = new EngineConfig(
                    config.getShardId(),
                    config.getThreadPool(),
                    config.getIndexSettings(),
                    config.getWarmer(),
                    config.getStore(),
                    config.getMergePolicy(),
                    config.getAnalyzer(),
                    config.getSimilarity(),
                    config.getCodecService(),
                    config.getEventListener(),
                    config.getQueryCache(),
                    config.getQueryCachingPolicy(),
                    newTranslogConfig,
                    config.getFlushMergesAfter(),
                    config.getExternalRefreshListener(),
                    config.getInternalRefreshListener(),
                    config.getIndexSort(),
                    config.getCircuitBreakerService(),
                    config.getGlobalCheckpointSupplier(),
                    config.retentionLeasesSupplier(),
                    config.getPrimaryTermSupplier(),
                    config.getSnapshotCommitSupplier(),
                    config.getLeafSorter(),
                    config.getRelativeTimeInNanosSupplier(),
                    config.getIndexCommitListener(),
                    config.isPromotableToPrimary()
                );
                return new IndexEngine(newConfig);
            } else {
                return new SearchEngine(config);
            }
        });
    }

    @Override
    public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
        return new StatelessShardRoutingRoleStrategy();
    }

    /**
     * Creates an {@link Engine.IndexCommitListener} that notifies the {@link ObjectStoreService} of all commit points created by Lucene.
     * This method is protected and overridable in tests.
     *
     * @return a {@link Engine.IndexCommitListener}
     */
    protected Engine.IndexCommitListener createIndexCommitListener() {
        final ObjectStoreService service = getObjectStoreService();
        return new Engine.IndexCommitListener() {
            @Override
            public void onNewCommit(
                ShardId shardId,
                Store store,
                long primaryTerm,
                Engine.IndexCommitRef indexCommitRef,
                Set<String> additionalFiles
            ) {
                store.incRef();
                Map<String, StoreFileMetadata> commitFiles;
                try {
                    Store.MetadataSnapshot metadata = store.getMetadata(indexCommitRef.getIndexCommit());
                    commitFiles = metadata.fileMetadataMap();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    store.decRef();
                }
                service.onCommitCreation(new StatelessCommitRef(shardId, indexCommitRef, commitFiles, additionalFiles, primaryTerm));
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {}
        };
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new StatelessAllocationDecider());
    }

    /**
     * Validates that stateless can work with the given node settings.
     */
    private static Settings requireValidSettings(final Settings settings) {
        if (STATELESS_ENABLED.get(settings) == false) {
            throw new IllegalArgumentException(NAME + " is not enabled");
        }
        var nonStatelessDataNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
            .stream()
            .filter(r -> r.canContainData() && STATELESS_ROLES.contains(r) == false)
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toSet());
        if (nonStatelessDataNodeRoles.isEmpty() == false) {
            throw new IllegalArgumentException(NAME + " does not support roles " + nonStatelessDataNodeRoles);
        }
        logger.info("{} is enabled", NAME);
        return settings;
    }
}
