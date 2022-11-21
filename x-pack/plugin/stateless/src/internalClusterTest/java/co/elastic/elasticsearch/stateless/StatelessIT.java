package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class, Stateless.class);
    }

    @BeforeClass
    public static void ensureStateless() {
        assertThat("Stateless feature flag must be enabled to run this test", DiscoveryNodeRole.hasStatelessFeatureFlag(), equalTo(true));
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), License.LicenseType.TRIAL.name())
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .build();
    }

    public void testClusterCanFormWithStatelessEnabled() {
        ensureStableCluster(internalCluster().size());

        var plugins = StreamSupport.stream(internalCluster().getInstances(PluginsService.class).spliterator(), false)
            .flatMap(ps -> ps.filterPlugins(Stateless.class).stream())
            .toList();
        assertThat(plugins.size(), greaterThan(0));
    }
}
