package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.lucene.DefaultDirectoryListener;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.lucene.StatelessDirectory;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Stateless extends Plugin {

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
        AllocationDeciders allocationDeciders
    ) {
        objectStoreService.set(new ObjectStoreService(settings, environment, repositoriesServiceSupplier));
        return List.of(objectStoreService.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(STATELESS_ENABLED, ObjectStoreService.TYPE, ObjectStoreService.BUCKET, ObjectStoreService.CLIENT);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // set a Lucene directory wrapper for all indices, so that stateless is notified of all operations on Lucene files
        indexModule.setDirectoryWrapper(StatelessDirectory::new);
        // register an IndexCommitListener so that stateless is notified of newly created commits on "index" nodes
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.INDEX_ROLE)) {
            indexModule.setIndexCommitListener(createIndexCommitListener());
        }
        // register a default listener when the shard is created in order to know the shard id and primary term
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                final StatelessDirectory directory = StatelessDirectory.unwrapDirectory(indexShard.store().directory());
                directory.addListener(new DefaultDirectoryListener(indexShard.shardId(), indexShard::getOperationPrimaryTerm));
            }
        });

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
                service.onCommitCreation(new StatelessCommitRef(shardId, indexCommitRef));
            }

            @Override
            public void onIndexCommitDelete(ShardId shardId, IndexCommit deletedCommit) {}
        };
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
