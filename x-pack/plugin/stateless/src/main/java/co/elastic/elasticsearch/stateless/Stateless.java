package co.elastic.elasticsearch.stateless;

import org.elasticsearch.Build;
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
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
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
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Stateless extends Plugin {

    private static final Logger logger = LogManager.getLogger(Stateless.class);

    private static final String NAME = "stateless";

    public static final LicensedFeature.Momentary STATELESS_FEATURE = LicensedFeature.momentary(
        null,
        NAME,
        License.OperationMode.ENTERPRISE
    );

    /** Setting for enabling stateless. Defaults to false. **/
    public static final Setting<Boolean> STATELESS_ENABLED = Setting.boolSetting(
        DiscoveryNode.STATELESS_ENABLED_SETTING_NAME,
        false,
        Setting.Property.NodeScope
    );

    static final Set<DiscoveryNodeRole> STATELESS_ROLES = Set.of(DiscoveryNodeRole.INDEX_ROLE, DiscoveryNodeRole.SEARCH_ROLE);

    private final Settings settings;
    private final StatelessLicenseChecker licenseChecker;
    private final boolean enabled;

    public Stateless(Settings settings) {
        this(
            settings,
            new StatelessLicenseChecker(() -> STATELESS_FEATURE.check(XPackPlugin.getSharedLicenseState())),
            Build.CURRENT.isSnapshot()
        );
    }

    Stateless(Settings settings, StatelessLicenseChecker statelessLicenseChecker, boolean isSnapshotBuild) {
        this(settings, statelessLicenseChecker, isSnapshotBuild, DiscoveryNodeRole.hasStatelessFeatureFlag());
    }

    Stateless(Settings settings, StatelessLicenseChecker statelessLicenseChecker, boolean isSnapshotBuild, boolean isStateless) {
        this.settings = Objects.requireNonNull(settings);
        this.enabled = validateSettings(settings, isSnapshotBuild, isStateless);
        this.licenseChecker = Objects.requireNonNull(statelessLicenseChecker);
    }

    boolean isEnabled() {
        return enabled;
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
        if (isEnabled()) {
            return List.of(licenseChecker);
        } else {
            return List.of();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        if (DiscoveryNodeRole.hasStatelessFeatureFlag()) {
            return List.of(STATELESS_ENABLED);
        } else {
            return List.of();
        }
    }

    /**
     * Validates that stateless can work with the given node settings.
     */
    private static boolean validateSettings(Settings settings, boolean isSnapshotBuild, boolean isStateless) {
        if (STATELESS_ENABLED.get(settings)) {
            if (isSnapshotBuild == false) {
                throw new IllegalArgumentException(NAME + " cannot be enabled in non-snapshot builds");
            }
            if (isStateless == false) {
                throw new IllegalArgumentException(NAME + " requires the feature flag [es.use_stateless] to be enabled");
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
            return true;
        } else {
            // stateless is not enabled, ensure the node has no stateless-related role
            STATELESS_ROLES.forEach(r -> {
                if (DiscoveryNode.hasRole(settings, r)) {
                    throw new IllegalArgumentException(
                        "Node role [" + r.roleName() + "] requires setting [" + STATELESS_ENABLED.getKey() + "] to be enabled"
                    );
                }
            });
            return false;
        }
    }
}
