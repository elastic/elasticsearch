package co.elastic.elasticsearch.stateless;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class StatelessSettingsTests extends ESTestCase {

    public void testEnabled() {
        assertThat(
            new Stateless(
                statelessSettings(List.of(randomFrom(Stateless.STATELESS_ROLES))),
                new StatelessLicenseChecker(() -> true),
                true,
                true
            ).isEnabled(),
            is(true)
        );
    }

    public void testDisabledByDefault() {
        assertThat(new Stateless(Settings.EMPTY).isEnabled(), is(false));
        assertThat(Stateless.STATELESS_ENABLED.get(Settings.EMPTY), is(false));
    }

    public void testSnapshotsBuildsOnly() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Stateless(
                statelessSettings(List.of(randomFrom(Stateless.STATELESS_ROLES))),
                new StatelessLicenseChecker(() -> true),
                false, // non-snapshot build
                true
            )
        );
        assertThat(exception.getMessage(), containsString("stateless cannot be enabled in non-snapshot builds"));
    }

    public void testStatelessNotInUse() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Stateless(
                statelessSettings(List.of(randomFrom(Stateless.STATELESS_ROLES))),
                new StatelessLicenseChecker(() -> true),
                true,
                false // use_stateless is false
            )
        );
        assertThat(exception.getMessage(), containsString("stateless requires the feature flag [es.use_stateless] to be enabled"));
    }

    public void testNonStatelessDataRolesNotAllowed() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new Stateless(
                statelessSettings(
                    List.of(
                        randomFrom(
                            DiscoveryNodeRole.roles()
                                .stream()
                                .filter(r -> r.canContainData() && Stateless.STATELESS_ROLES.contains(r) == false)
                                .toList()
                        )
                    )
                ),
                new StatelessLicenseChecker(() -> true),
                true,
                true
            )
        );
        assertThat(exception.getMessage(), containsString("stateless does not support roles ["));
    }

    private static Settings statelessSettings(Collection<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder();
        builder.put(Stateless.STATELESS_ENABLED.getKey(), true);
        if (roles != null) {
            builder.putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), roles.stream().map(DiscoveryNodeRole::roleName).toList());
        }
        return builder.build();
    }
}
