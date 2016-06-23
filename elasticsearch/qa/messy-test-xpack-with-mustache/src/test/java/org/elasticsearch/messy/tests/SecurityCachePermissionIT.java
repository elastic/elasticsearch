/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.script.ScriptService.ScriptType.INLINE;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@SecurityIntegTestCase.AwaitsFix(bugUrl = "clean up test to not use mustache templates, otherwise needs many resources here")
public class SecurityCachePermissionIT extends SecurityIntegTestCase {
    static final String READ_ONE_IDX_USER = "read_user";
    
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> types = new ArrayList<>();
        types.addAll(super.nodePlugins());
        types.add(MustachePlugin.class);
        return types;
    }

    @Override
    public String configUsers() {
        return super.configUsers()
                + READ_ONE_IDX_USER + ":" + SecuritySettingsSource.DEFAULT_PASSWORD_HASHED + "\n";
    }

    @Override
    public String configRoles() {
        return super.configRoles()
                + "\nread_one_idx:\n"
                + "  indices:\n"
                + "    'data':\n"
                + "      - read\n";
    }

    @Override
    public String configUsersRoles() {
        return super.configUsersRoles()
                + "read_one_idx:" + READ_ONE_IDX_USER + "\n";
    }

    @BeforeClass
    public static void checkVersion() {
        assumeTrue("These tests are only valid with elasticsearch 1.6.0+", Version.CURRENT.id >= 1060099);
    }

    @Before
    public void loadData() {
        index("data", "a", "1", "{ \"name\": \"John\", \"token\": \"token1\" }");
        index("tokens", "tokens", "1", "{ \"group\": \"1\", \"tokens\": [\"token1\", \"token2\"] }");
        refresh();
    }

    public void testThatTermsFilterQueryDoesntLeakData() {
        SearchResponse response = client().prepareSearch("data").setTypes("a").setQuery(QueryBuilders.constantScoreQuery(
                QueryBuilders.termsLookupQuery("token", new TermsLookup("tokens", "tokens", "1", "tokens"))))
                .execute().actionGet();
        assertThat(response.isTimedOut(), is(false));
        assertThat(response.getHits().hits().length, is(1));

        // Repeat with unauthorized user!!!!
        try {
            response = client().filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(READ_ONE_IDX_USER,
                    new SecuredString("changeme".toCharArray()))))
                    .prepareSearch("data").setTypes("a").setQuery(QueryBuilders.constantScoreQuery(
                    QueryBuilders.termsLookupQuery("token", new TermsLookup("tokens", "tokens", "1", "tokens"))))
                    .execute().actionGet();
            fail("search phase exception should have been thrown! response was:\n" + response.toString());
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.toString(), containsString("ElasticsearchSecurityException[action"));
            assertThat(e.toString(), containsString("unauthorized"));
        }
    }

    public void testThatScriptServiceDoesntLeakData() {
        String source = "{\n" +
                "\"template\": {\n" +
                "  \"query\": {\n" +
                "    \"exists\": {\n" +
                "      \"field\": \"{{name}}\"\n" +
                "     }\n" +
                "   }\n" +
                " }\n" +
                "}";

        //Template template = new Template(source, INLINE, MustacheScriptEngineService.NAME, null, singletonMap("name", "token"));
        SearchResponse response = client().prepareSearch("data").setTypes("a")
                .setQuery(QueryBuilders.templateQuery(source, singletonMap("name", "token")))
                .execute().actionGet();
        assertThat(response.isTimedOut(), is(false));
        assertThat(response.getHits().hits().length, is(1));

        // Repeat with unauthorized user!!!!
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(READ_ONE_IDX_USER,
                        new SecuredString("changeme".toCharArray()))))
                    .prepareSearch("data").setTypes("a")
                    .setQuery(QueryBuilders.templateQuery(source, singletonMap("name", "token")))
                    .execute().actionGet());
        assertThat(e.toString(), containsString("ElasticsearchSecurityException[action"));
        assertThat(e.toString(), containsString("unauthorized"));
    }
}
