/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.security.authc.kerberos.KerberosRealmTestCase;
import org.elasticsearch.xpack.security.authc.saml.SamlRealmTestHelper;
import org.hamcrest.Matchers;
import org.junit.AfterClass;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * An integration test that configures one of each realm type.
 * This acts as a basic smoke test that every realm is supported, and can be configured.
 */
public class SecurityRealmSettingsTests extends SecurityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings settings;
        try {
            final String samlIdpEntityId = "urn:idp:entity";
            final Path samlIdpPath = createTempFile("idp", "xml");
            SamlRealmTestHelper.writeIdpMetaData(samlIdpPath, samlIdpEntityId);

            final Path kerbKeyTab = createTempFile("es", "keytab");
            KerberosRealmTestCase.writeKeyTab(kerbKeyTab, null);

            settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal).filter(s -> s.startsWith("xpack.security.authc.realms.") == false))
                .put("xpack.security.authc.token.enabled", true)
                .put("xpack.security.authc.realms.file.file1.order", 1)
                .put("xpack.security.authc.realms.native.native1.order", 2)
                .put("xpack.security.authc.realms.ldap.ldap1.order", 3)
                .put("xpack.security.authc.realms.ldap.ldap1.url", "ldap://127.0.0.1:389")
                .put("xpack.security.authc.realms.ldap.ldap1.user_dn_templates", "cn={0},dc=example,dc=com")
                .put("xpack.security.authc.realms.active_directory.ad1.order", 4)
                .put("xpack.security.authc.realms.active_directory.ad1.url", "ldap://127.0.0.1:389")
                .put("xpack.security.authc.realms.pki.pki1.order", 5)
                .put("xpack.security.authc.realms.saml.saml1.order", 6)
                .put("xpack.security.authc.realms.saml.saml1.idp.metadata.path", samlIdpPath.toAbsolutePath())
                .put("xpack.security.authc.realms.saml.saml1.idp.entity_id", samlIdpEntityId)
                .put("xpack.security.authc.realms.saml.saml1.sp.entity_id", "urn:sp:entity")
                .put("xpack.security.authc.realms.saml.saml1.sp.acs", "http://localhost/acs")
                .put("xpack.security.authc.realms.saml.saml1.attributes.principal", "uid")
                .put("xpack.security.authc.realms.kerberos.kerb1.order", 7)
                .put("xpack.security.authc.realms.kerberos.kerb1.keytab.path", kerbKeyTab.toAbsolutePath())
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Set<String> configuredRealmTypes = RealmSettings.getRealmSettings(settings)
            .keySet()
            .stream()
            .map(RealmConfig.RealmIdentifier::getType)
            .collect(Collectors.toSet());
        assertThat("One or more realm type are not configured " + configuredRealmTypes,
            configuredRealmTypes, Matchers.containsInAnyOrder(InternalRealms.getConfigurableRealmsTypes().toArray(Strings.EMPTY_ARRAY)));

        return settings;
    }

    /**
     * Some realms (currently only SAML, but maybe more in the future) hold on to resources that may need to be explicitly closed.
     */
    @AfterClass
    public static void closeRealms() throws IOException {
        final Logger logger = LogManager.getLogger(SecurityRealmSettingsTests.class);
        final Iterable<Realms> realms = internalCluster().getInstances(Realms.class);
        for (Realms rx : realms) {
            for (Realm r : rx) {
                if (r instanceof Closeable) {
                    logger.info("Closing realm [{}] [{} @ {}]", r, r.getClass().getSimpleName(), System.identityHashCode(r));
                    ((Closeable) r).close();
                }
            }
        }
    }

    /**
     * Always enable transport SSL so that it is possible to have a PKI Realm
     */
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testClusterStarted() {
        final AuthenticateRequest request = new AuthenticateRequest();
        request.username(nodeClientUsername());
        final AuthenticateResponse authenticate = client().execute(AuthenticateAction.INSTANCE, request).actionGet(10, TimeUnit.SECONDS);
        assertThat(authenticate.authentication(), notNullValue());
        assertThat(authenticate.authentication().getUser(), notNullValue());
        assertThat(authenticate.authentication().getUser().enabled(), is(true));
    }

}
