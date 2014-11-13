/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class SSLServiceTests extends ElasticsearchTestCase {

    File testnodeStore;

    @Before
    public void setup() throws Exception {
        testnodeStore = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
    }

    @Test(expected = ElasticsearchSSLException.class)
    public void testThatInvalidProtocolThrowsException() throws Exception {
        new SSLService(settingsBuilder()
                            .put("shield.ssl.protocol", "non-existing")
                            .put("shield.ssl.keystore", testnodeStore.getPath())
                            .put("shield.ssl.keystore_password", "testnode")
                            .put("shield.ssl.truststore", testnodeStore.getPath())
                            .put("shield.ssl.truststore_password", "testnode")
                        .build());
    }

    @Test @Ignore //TODO it appears that setting a specific protocol doesn't make a difference
    public void testSpecificProtocol() {
        SSLService ssl = new SSLService(settingsBuilder()
                .put("shield.ssl.protocol", "TLSv1.2")
                .put("shield.ssl.keystore", testnodeStore.getPath())
                .put("shield.ssl.keystore_password", "testnode")
                .put("shield.ssl.truststore", testnodeStore.getPath())
                .put("shield.ssl.truststore_password", "testnode")
                .build());
        assertThat(ssl.createSSLEngine().getSSLParameters().getProtocols(), arrayContaining("TLSv1.2"));
    }

    @Test
    public void testIsSSLDisabled_allDefaults(){
        Settings settings = settingsBuilder().build();
        assertSSLDisabled(settings);
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapOff(){

        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertSSLDisabled(settings);
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapMissingUrl() {
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "active_dir") //SSL is on by default for a missing URL with active directory
                .build();
        assertSSLEnabled(settings);
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapOn(){
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldaps://example.com:636")
                .build();
        assertSSLEnabled(settings);
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOnLdapOff(){

        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", true)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertSSLEnabled(settings);
    }

    @Test
    public void testIsSSLEnabled_transportOnHttpOffLdapOff(){
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertSSLEnabled(settings);
    }

    private void assertSSLEnabled(Settings settings) {
        assertThat(SSLService.isSSLEnabled(settings), is(true));
    }

    private void assertSSLDisabled(Settings settings) {
        assertThat(SSLService.isSSLEnabled(settings), is(false));
    }
}
