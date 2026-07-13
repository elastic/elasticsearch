/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This tests that configurations that contain two AD realms work correctly.
 * The required behaviour is that users from both realms (directory servers) can be authenticated using
 * just their userid (the AuthenticationService tries them in order)
 */
public class MultipleAdRealmIT extends AbstractAdLdapRealmTestCase {

    private static RealmConfig secondaryRealmConfig;

    @BeforeClass
    public static void setupSecondaryRealm() {
        // Pick a secondary realm that has the inverse value for 'loginWithCommonName' compare with the primary realm
        final List<RealmConfig> configs = Arrays.stream(RealmConfig.values())
            .filter(config -> config.loginWithCommonName != AbstractAdLdapRealmTestCase.realmConfig.loginWithCommonName)
            .filter(config -> config.name().startsWith("AD"))
            .collect(Collectors.toList());
        secondaryRealmConfig = randomFrom(configs);
        LogManager.getLogger(MultipleAdRealmIT.class)
            .info(
                "running test with secondary realm configuration [{}], with direct group to role mapping [{}]. Settings [{}]",
                secondaryRealmConfig,
                secondaryRealmConfig.mapGroupsAsRoles,
                secondaryRealmConfig.settings
            );

        // It's easier to test 2 realms when using file based role mapping, and for the purposes of
        // this test, there's no need to test native mappings.
        AbstractAdLdapRealmTestCase.roleMappings = realmConfig.selectRoleMappings(() -> true);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder();
        builder.put(super.nodeSettings(nodeOrdinal, otherSettings));

        final List<RoleMappingEntry> secondaryRoleMappings = secondaryRealmConfig.selectRoleMappings(() -> true);
        final Settings secondarySettings = super.buildRealmSettings(
            secondaryRealmConfig,
            secondaryRoleMappings,
            getNodeTrustedCertificates()
        );
        secondarySettings.keySet().forEach(name -> {
            final String newname;
            if (name.contains(LdapRealmSettings.AD_TYPE)) {
                newname = name.replace(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL, XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + "2");
            } else {
                newname = name.replace(XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL, XPACK_SECURITY_AUTHC_REALMS_LDAP_EXTERNAL + "2");
            }
            builder.copy(newname, name, secondarySettings);
        });

        return builder.build();
    }

    /**
     * Test that both realms support user login. Implementation wise, this means that if the first realm reject the authentication attempt,
     * then the second realm will be tried.
     * Because one realm is using "common name" (cn) for login, and the other uses the "userid" (sAMAccountName) [see
     * {@link #setupSecondaryRealm()}], this is simply a matter of checking that we can authenticate with both identifiers.
     */
    public void testCanAuthenticateAgainstBothRealms() throws IOException {
        assertAccessAllowed("Natasha Romanoff", "avengers");
        assertAccessAllowed("blackwidow", "avengers");
    }

}
