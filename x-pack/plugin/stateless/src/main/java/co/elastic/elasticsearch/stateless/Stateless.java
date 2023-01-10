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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.lucene.DefaultDirectoryListener;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.lucene.StatelessDirectory;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardRecoveryPlan;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;

public class Stateless extends Plugin implements EnginePlugin, RecoveryPlannerPlugin, ActionPlugin {

    private static final Logger logger = LogManager.getLogger(Stateless.class);

    public static final String NAME = "stateless";

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    private final SetOnce<ObjectStoreService> objectStoreService = new SetOnce<>();
    private final Settings settings;

    private ObjectStoreService getObjectStoreService() {
        return Objects.requireNonNull(this.objectStoreService.get());
    }

    public Stateless(Settings settings) {
        this.settings = requireValidSettings(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(NewCommitNotificationAction.INSTANCE, TransportNewCommitNotificationAction.class));
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
        var objectStoreService = new ObjectStoreService(settings, repositoriesServiceSupplier, threadPool, client, clusterService);
        this.objectStoreService.set(objectStoreService);
        return List.of(objectStoreService);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            STATELESS_ENABLED,
            ObjectStoreService.TYPE_SETTING,
            ObjectStoreService.BUCKET_SETTING,
            ObjectStoreService.CLIENT_SETTING,
            IndexEngine.INDEX_FLUSH_INTERVAL_SETTING,
            ObjectStoreService.OBJECT_STORE_SHUTDOWN_TIMEOUT
        );
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // set a Lucene directory wrapper for all indices, so that stateless is notified of all operations on Lucene files
        indexModule.setDirectoryWrapper(StatelessDirectory::new);
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE)) {
            indexModule.setIndexCommitListener(createIndexCommitListener());
        }
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void beforeIndexShardRecovery(IndexShard indexShard, IndexSettings indexSettings) {
                if (indexShard.recoveryState().getPrimary() == false) {
                    final Store store = indexShard.store();
                    store.incRef();
                    try {
                        // creates a new empty Lucene index
                        Lucene.cleanLuceneIndex(store.directory());
                        // TODO Download files from object store here and only create an empty store if no blobs are downloaded
                        store.createEmpty();
                        // not required but avoid annoying warnings in logs
                        final String translogUUID = Translog.createEmptyTranslog(
                            indexShard.shardPath().resolveTranslog(),
                            indexShard.shardId(),
                            SequenceNumbers.NO_OPS_PERFORMED,
                            indexShard.getPendingPrimaryTerm(),
                            "_na_",
                            null
                        );
                        store.associateIndexWithNewTranslog(translogUUID);
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to create empty Lucene index", e);
                    } finally {
                        store.decRef();
                    }
                }
            }

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                final StatelessDirectory directory = StatelessDirectory.unwrapDirectory(indexShard.store().directory());
                // register a default listener when the shard is created in order to log all operations on Lucene files
                directory.addListener(new DefaultDirectoryListener(indexShard.shardId(), indexShard::getOperationPrimaryTerm));
            }
        });
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.of(config -> config.isRecoveringAsPrimary() ? new IndexEngine(config) : new SearchEngine(config));
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
            public void onNewCommit(ShardId shardId, long primaryTerm, Engine.IndexCommitRef indexCommitRef, Set<String> additionalFiles) {
                service.onCommitCreation(new StatelessCommitRef(shardId, indexCommitRef, additionalFiles, primaryTerm));
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {}
        };
    }

    @Override
    public Optional<RecoveryPlannerService> createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService) {
        return Optional.of(
            (
                shardId,
                shardStateIdentifier,
                sourceMetadata,
                targetMetadata,
                startingSeqNo,
                translogOps,
                targetVersion,
                useSnapshots,
                primaryRelocation,
                listener) -> {
                if (primaryRelocation) {
                    // TODO index/primary shards should also use object store instead of file based recovery
                    ActionListener.completeWith(listener, () -> {
                        Store.RecoveryDiff diff = sourceMetadata.recoveryDiff(targetMetadata);
                        return new ShardRecoveryPlan(
                            ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY,
                            concatLists(diff.missing, diff.different),
                            diff.identical,
                            startingSeqNo,
                            translogOps,
                            sourceMetadata
                        );
                    });
                } else {
                    // create an empty recovery plan for search/replica shards
                    ActionListener.completeWith(
                        listener,
                        () -> new ShardRecoveryPlan(
                            ShardRecoveryPlan.SnapshotFilesToRecover.EMPTY, // no files to recovery from snapshot
                            List.of(), // no files to recover from peer
                            List.of(), // no files in target too
                            startingSeqNo,
                            translogOps,
                            sourceMetadata
                        )
                    );
                }
            }
        );
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
