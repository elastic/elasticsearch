/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.Attribute;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class LdapMetadataResolverTests extends ESTestCase {

    private static final String HAWKEYE_DN = "uid=hawkeye,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";

    private LdapMetadataResolver resolver;

    public void testParseSettings() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "my_ldap");
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .putList(
                RealmSettings.getFullSettingKey(
                    realmId.getName(),
                    LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING.apply(LdapRealmSettings.LDAP_TYPE)
                ),
                "cn",
                "uid"
            )
            .put(RealmSettings.getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = new RealmConfig(realmId, settings, TestEnvironment.newEnvironment(settings), new ThreadContext(settings));
        resolver = new LdapMetadataResolver(config, false);
        assertThat(resolver.attributeNames(), arrayContainingInAnyOrder("cn", "uid"));
    }

    public void testResolveSingleValuedAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetadataResolver(null, null, Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Arrays.asList(
            new Attribute("cn", "Clint Barton"),
            new Attribute("uid", "hawkeye"),
            new Attribute("email", "clint.barton@shield.gov"),
            new Attribute("memberOf", "cn=staff,ou=groups,dc=example,dc=com", "cn=admin,ou=groups,dc=example,dc=com")
        );
        final Map<String, Object> map = resolve(attributes);
        assertThat(map, aMapWithSize(2));
        assertThat(map.get("cn"), equalTo("Clint Barton"));
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    public void testResolveMultiValuedAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetadataResolver(null, null, Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Arrays.asList(
            new Attribute("cn", "Clint Barton", "hawkeye"),
            new Attribute("uid", "hawkeye")
        );
        final Map<String, Object> map = resolve(attributes);
        assertThat(map, aMapWithSize(2));
        assertThat(map.get("cn"), instanceOf(List.class));
        assertThat((List<?>) map.get("cn"), contains("Clint Barton", "hawkeye"));
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    public void testResolveMissingAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetadataResolver(null, null, Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Collections.singletonList(new Attribute("uid", "hawkeye"));
        final Map<String, Object> map = resolve(attributes);
        assertThat(map, aMapWithSize(1));
        assertThat(map.get("cn"), nullValue());
        assertThat(map.get("uid"), equalTo("hawkeye"));
    }

    private Map<String, Object> resolve(Collection<Attribute> attributes) throws Exception {
        final PlainActionFuture<LdapMetadataResolver.LdapMetadataResult> future = new PlainActionFuture<>();
        resolver.resolve(null, HAWKEYE_DN, TimeValue.timeValueSeconds(1), logger, attributes, future);
        return future.get().getMetaData();
    }
}
