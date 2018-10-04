/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.unboundid.ldap.sdk.Attribute;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class LdapMetaDataResolverTests extends ESTestCase {

    private static final String HAWKEYE_DN = "uid=hawkeye,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";

    private LdapMetaDataResolver resolver;

    public void testParseSettings() throws Exception {
        resolver = new LdapMetaDataResolver(Settings.builder().putList("metadata", "cn", "uid").build(), false);
        assertThat(resolver.attributeNames(), arrayContaining("cn", "uid"));
    }

    public void testResolveSingleValuedAttributeFromCachedAttributes() throws Exception {
        resolver = new LdapMetaDataResolver(Arrays.asList("cn", "uid"), true);
        final Collection<Attribute> attributes = Arrays.asList(
                new Attribute("cn", "Clint Barton"),
                new Attribute("uid", "hawkeye"),
                new Attribute("email", "clint.barton@shield.gov"),
                new Attribute("memberOf", "cn=staff,ou=groups,dc=example,dc=com", "cn=admin,ou=groups,dc=example,dc=com")
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

    private Map<String, Object> resolve(Collection<Attribute> attributes) throws Exception {
        final PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        resolver.resolve(null, HAWKEYE_DN, TimeValue.timeValueSeconds(1), logger, attributes, future);
        return future.get();
    }
}
