/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import javax.security.auth.DestroyFailedException;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;
import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.security.authc.ldap.LdapTestUtils;
import org.elasticsearch.xpack.security.authc.ldap.OpenLdapTests;
import org.junit.After;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class LdapMetaDataResolverTests extends ESTestCase {

    private static final String HAWKEYE_DN = "uid=hawkeye,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";

    private LdapMetaDataResolver resolver;
    private LDAPConnection connection;

    public void testParseSettings() throws Exception {
        resolver = new LdapMetaDataResolver(Settings.builder().putArray("metadata", "cn", "uid").build(), false);
        assertThat(resolver.attributeNames(), arrayContaining("cn", "uid"));
    }

    public void testResolveSingleValuedAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Arrays.asList(
                new Attribute("cn", "Clint Barton"),
                new Attribute("uid", "hawkeye"),
                new Attribute("email", "clint.barton@shield.gov"),
                new Attribute("memberOf", "cn=staff,ou=groups,dc=exmaple,dc=com", "cn=admin,ou=groups,dc=exmaple,dc=com")
        );
        final Map<String, Object> map = resolve(attributes);
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("cn"), equalTo("Clint Barton"));
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    public void testResolveMultiValuedAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Arrays.asList(
                new Attribute("cn", "Clint Barton", "hawkeye"),
                new Attribute("uid", "hawkeye")
        );
        final Map<String, Object> map = resolve(attributes);
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("cn"), instanceOf(List.class));
        assertThat((List<?>) map.get("cn"), contains("Clint Barton", "hawkeye"));
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    public void testResolveMissingAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Collections.singletonList(new Attribute("uid", "hawkeye"));
        final Map<String, Object> map = resolve(attributes);
        assertThat(map.size(), equalTo(1));
        assertThat(map.get("cn"), nullValue());
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    @Network
    @AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/1823")
    public void testResolveSingleValuedAttributeFromConnection() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("givenName", "sn"), true);
        setupOpenLdapConnection();
        final Map<String, Object> map = resolve(null);
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("givenName"), equalTo("Clint"));
        assertThat(map.get("sn"), equalTo("Barton"));
    }

    @Network
    @AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/1823")
    public void testResolveMultiValuedAttributeFromConnection() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("objectClass"), true);
        setupOpenLdapConnection();
        final Map<String, Object> map = resolve(null);
        assertThat(map.size(), equalTo(1));
        assertThat(map.get("objectClass"), instanceOf(List.class));
        assertThat((List<?>) map.get("objectClass"), contains("top", "posixAccount", "inetOrgPerson"));
    }

    @Network
    @AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/1823")
    public void testResolveMissingAttributeFromConnection() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("alias"), true);
        setupOpenLdapConnection();
        final Map<String, Object> map = resolve(null);
        assertThat(map.size(), equalTo(0));
    }

    private Map<String, Object> resolve(Collection<Attribute> attributes) throws Exception {
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        resolver.resolve(connection, HAWKEYE_DN, TimeValue.timeValueSeconds(1), logger, attributes, future);
        return future.get();
    }

    private void setupOpenLdapConnection() throws Exception {
        Path truststore = getDataPath("./ldaptrust.jks");
        this.connection = LdapTestUtils.openConnection(OpenLdapTests.OPEN_LDAP_URL, HAWKEYE_DN, OpenLdapTests.PASSWORD, truststore);
    }

    @After
    public void tearDownLdapConnection() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }
}