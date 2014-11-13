/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.security.NoSuchAlgorithmException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class SSLServiceTests extends ElasticsearchTestCase {

    File testnodeStore;

    @Before
    public void setup() throws Exception {
        testnodeStore = new File(getClass().getResource("/org/elasticsearch/shield/transport/ssl/certs/simple/testnode.jks").toURI());
    }

    @Test
    public void testThatInvalidProtocolThrowsException() throws Exception {
        try {
            new SSLService(settingsBuilder()
                            .put("shield.ssl.protocol", "non-existing")
                            .put("shield.ssl.keystore", testnodeStore.getPath())
                            .put("shield.ssl.keystore_password", "testnode")
                            .put("shield.ssl.truststore", testnodeStore.getPath())
                            .put("shield.ssl.truststore_password", "testnode")
                        .build());
        } catch (ElasticsearchSSLException e) {
            Assert.assertThat(e.getRootCause(), Matchers.instanceOf(NoSuchAlgorithmException.class));
        }
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
        Assert.assertThat(ssl.createSSLEngine().getSSLParameters().getProtocols(), Matchers.arrayContaining("TLSv1.2"));
    }

    @Test
    public void testIsSSLEnabled_allDefaults(){
        Settings settings = settingsBuilder().build();
        assertThat(SSLService.isSSLEnabled(settings), is(false));
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapOff(){

        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertThat(SSLService.isSSLEnabled(settings), is(false));
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapMissingUrl() {
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "active_dir") //default for missing URL is
                .build();
        assertThat(SSLService.isSSLEnabled(settings), is(true));
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOffLdapOn(){
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldaps://example.com:636")
                .build();
        assertThat(SSLService.isSSLEnabled(settings), is(true));
    }

    @Test
    public void testIsSSLEnabled_transportOffHttpOnLdapOff(){

        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", false)
                .put("shield.http.ssl", true)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertThat(SSLService.isSSLEnabled(settings), is(true));
    }

    @Test
    public void testIsSSLEnabled_transportOnHttpOffLdapOff(){
        Settings settings = settingsBuilder()
                .put("shield.transport.ssl", true)
                .put("shield.http.ssl", false)
                .put("shield.authc.ldap.mode", "ldap")
                .put("shield.authc.ldap.url", "ldap://example.com:389")
                .build();
        assertThat(SSLService.isSSLEnabled(settings), is(true));
    }
}
