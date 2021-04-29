/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.junit.Before;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SecurityCachePermissionTests extends SecurityIntegTestCase {

    private final String READ_ONE_IDX_USER = "read_user";

    @Override
    public String configUsers() {
        return super.configUsers()
                + READ_ONE_IDX_USER + ":" + SecuritySettingsSource.TEST_PASSWORD_HASHED + "\n";
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

    @Before
    public void loadData() {
        index("data", "1", "{ \"name\": \"John\", \"token\": \"token1\" }");
        index("tokens", "1", "{ \"group\": \"1\", \"tokens\": [\"token1\", \"token2\"] }");
        refresh();
    }

    public void testThatTermsFilterQueryDoesntLeakData() {
        SearchResponse response = client().prepareSearch("data").setQuery(QueryBuilders.constantScoreQuery(
                QueryBuilders.termsLookupQuery("token", new TermsLookup("tokens", "1", "tokens"))))
                .execute().actionGet();
        assertThat(response.isTimedOut(), is(false));
        assertThat(response.getHits().getHits().length, is(1));

        // Repeat with unauthorized user!!!!
        try {
            response = client().filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(READ_ONE_IDX_USER,
                    SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)))
                    .prepareSearch("data")
                    .setQuery(QueryBuilders.constantScoreQuery(
                    QueryBuilders.termsLookupQuery("token", new TermsLookup("tokens", "1", "tokens"))))
                    .execute().actionGet();
            fail("search phase exception should have been thrown! response was:\n" + response.toString());
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.toString(), containsString("ElasticsearchSecurityException: action"));
            assertThat(e.toString(), containsString("unauthorized"));
        }
    }
}
