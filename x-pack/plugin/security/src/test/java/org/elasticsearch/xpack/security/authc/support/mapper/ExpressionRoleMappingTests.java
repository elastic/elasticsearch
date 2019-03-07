/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.security.authc.support.UserRoleMapper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class ExpressionRoleMappingTests extends ESTestCase {

    private RealmConfig realm;

    @Before
    public void setupMapping() throws Exception {
        realm = new RealmConfig(new RealmConfig.RealmIdentifier("ldap", "ldap1"),
                Settings.EMPTY, Mockito.mock(Environment.class), new ThreadContext(Settings.EMPTY));
    }

    public void testParseValidJson() throws Exception {
        String json = "{"
                + "\"roles\": [  \"kibana_user\", \"sales\" ], "
                + "\"enabled\": true, "
                + "\"rules\": { "
                + "  \"all\": [ "
                + "    { \"field\": { \"dn\" : \"*,ou=sales,dc=example,dc=com\" } }, "
                + "    { \"except\": { \"field\": { \"metadata.active\" : false } } }"
                + "  ]}"
                + "}";
        final ExpressionRoleMapping mapping = parse(json, "ldap_sales");
        assertThat(mapping.getRoles(), Matchers.containsInAnyOrder("kibana_user", "sales"));
        assertThat(mapping.getExpression(), instanceOf(AllExpression.class));

        final UserRoleMapper.UserData user1a = new UserRoleMapper.UserData(
                "john.smith", "cn=john.smith,ou=sales,dc=example,dc=com",
                Collections.emptyList(), Collections.singletonMap("active", true), realm
        );
        final UserRoleMapper.UserData user1b = new UserRoleMapper.UserData(
                user1a.getUsername(), user1a.getDn().toUpperCase(Locale.US), user1a.getGroups(), user1a.getMetadata(), user1a.getRealm()
        );
        final UserRoleMapper.UserData user1c = new UserRoleMapper.UserData(
                user1a.getUsername(), user1a.getDn().replaceAll(",", ", "), user1a.getGroups(), user1a.getMetadata(), user1a.getRealm()
        );
        final UserRoleMapper.UserData user1d = new UserRoleMapper.UserData(
                user1a.getUsername(), user1a.getDn().replaceAll("dc=", "DC="), user1a.getGroups(), user1a.getMetadata(), user1a.getRealm()
        );
        final UserRoleMapper.UserData user2 = new UserRoleMapper.UserData(
                "jamie.perez", "cn=jamie.perez,ou=sales,dc=example,dc=com",
                Collections.emptyList(), Collections.singletonMap("active", false), realm
        );

        final UserRoleMapper.UserData user3 = new UserRoleMapper.UserData(
                "simone.ng", "cn=simone.ng,ou=finance,dc=example,dc=com",
                Collections.emptyList(), Collections.singletonMap("active", true), realm
        );

        assertThat(mapping.getExpression().match(user1a.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1b.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1c.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1d.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user2.asModel()), equalTo(false));
        assertThat(mapping.getExpression().match(user3.asModel()), equalTo(false));
    }

    public void testParsingFailsIfRulesAreMissing() throws Exception {
        String json = "{"
                + "\"roles\": [  \"kibana_user\", \"sales\" ], "
                + "\"enabled\": true "
                + "}";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("rules"));
    }

    public void testParsingFailsIfRolesMissing() throws Exception {
        String json = "{"
                + "\"enabled\": true, "
                + "\"rules\": "
                + "    { \"field\": { \"dn\" : \"*,ou=sales,dc=example,dc=com\" } } "
                + "}";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("role"));
    }

    public void testParsingFailsIfThereAreUnrecognisedFields() throws Exception {
        String json = "{"
                + "\"disabled\": false, "
                + "\"roles\": [  \"kibana_user\", \"sales\" ], "
                + "\"rules\": "
                + "    { \"field\": { \"dn\" : \"*,ou=sales,dc=example,dc=com\" } } "
                + "}";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("disabled"));
    }

    public void testParsingIgnoresTypeFields() throws Exception {
        String json = "{"
                + "\"enabled\": true, "
                + "\"roles\": [  \"kibana_user\", \"sales\" ], "
                + "\"rules\": "
                + "    { \"field\": { \"dn\" : \"*,ou=sales,dc=example,dc=com\" } }, "
                + "\"doc_type\": \"role-mapping\", "
                + "\"type\": \"doc\""
                + "}";
        final ExpressionRoleMapping mapping = parse(json, "from_index");
        assertThat(mapping.isEnabled(), equalTo(true));
        assertThat(mapping.getRoles(), containsInAnyOrder("kibana_user", "sales"));
    }

    private ExpressionRoleMapping parse(String json, String name) throws IOException {
        final NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;
        final XContentParser parser = XContentType.JSON.xContent()
                .createParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        final ExpressionRoleMapping mapping = ExpressionRoleMapping.parse(name, parser);
        assertThat(mapping, notNullValue());
        assertThat(mapping.getName(), equalTo(name));
        return mapping;
    }

}
