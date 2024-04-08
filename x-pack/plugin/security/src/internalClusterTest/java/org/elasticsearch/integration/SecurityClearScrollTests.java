/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SecurityClearScrollTests extends SecurityIntegTestCase {

    private List<String> scrollIds;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(
            getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        return super.configUsers() + "allowed_user:" + usersPasswdHashed + "\n" + "denied_user:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "allowed_role:allowed_user\n" + "denied_role:denied_user\n";
    }

    @Override
    protected String configRoles() {
        return Strings.format("""
            %s
            allowed_role:
              cluster:
                - cluster:admin/indices/scroll/clear_all\s
            denied_role:
              indices:
                - names: '*'
                  privileges: [ALL]
            """, super.configRoles());
    }

    @Before
    public void indexRandomDocuments() {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(IMMEDIATE);
        for (int i = 0; i < randomIntBetween(10, 50); i++) {
            bulkRequestBuilder.add(prepareIndex("index").setId(String.valueOf(i)).setSource("{ \"foo\" : \"bar\" }", XContentType.JSON));
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        assertThat(bulkItemResponses.hasFailures(), is(false));

        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        int count = randomIntBetween(5, 15);
        for (int i = 0; i < count; i++) {
            multiSearchRequestBuilder.add(prepareSearch("index").setScroll("10m").setSize(1));
        }
        scrollIds = new ArrayList<>();
        assertResponse(multiSearchRequestBuilder, multiSearchResponse -> scrollIds.addAll(getScrollIds(multiSearchResponse)));
    }

    @After
    public void clearScrolls() {
        // clear all scroll ids from the default admin user, just in case any of test fails
        client().prepareClearScroll().addScrollId("_all").get();
    }

    public void testThatClearingAllScrollIdsWorks() throws Exception {
        String user = "allowed_user:" + SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        String basicAuth = basicAuthHeaderValue("allowed_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        Map<String, String> headers = new HashMap<>();
        headers.put(SecurityField.USER_SETTING.getKey(), user);
        headers.put(BASIC_AUTH_HEADER, basicAuth);
        ClearScrollResponse clearScrollResponse = client().filterWithHeader(headers).prepareClearScroll().addScrollId("_all").get();
        assertThat(clearScrollResponse.isSucceeded(), is(true));

        assertThatScrollIdsDoNotExist();
    }

    public void testThatClearingAllScrollIdsRequirePermissions() throws Exception {
        String user = "denied_user:" + SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        String basicAuth = basicAuthHeaderValue("denied_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        Map<String, String> headers = new HashMap<>();
        headers.put(SecurityField.USER_SETTING.getKey(), user);
        headers.put(BASIC_AUTH_HEADER, basicAuth);
        assertRequestBuilderThrows(
            client().filterWithHeader(headers).prepareClearScroll().addScrollId("_all"),
            ElasticsearchSecurityException.class,
            "action [cluster:admin/indices/scroll/clear_all] is unauthorized for user [denied_user]"
        );

        // deletion of scroll ids should work
        ClearScrollResponse clearByIdScrollResponse = client().prepareClearScroll().setScrollIds(scrollIds).get();
        assertThat(clearByIdScrollResponse.isSucceeded(), is(true));

        // test with each id, that they do not exist
        assertThatScrollIdsDoNotExist();
    }

    private void assertThatScrollIdsDoNotExist() {
        for (String scrollId : scrollIds) {
            SearchPhaseExecutionException expectedException = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearchScroll(scrollId).get()
            );
            assertThat(expectedException.toString(), containsString("SearchContextMissingException"));
        }
    }

    private List<String> getScrollIds(MultiSearchResponse multiSearchResponse) {
        List<String> ids = new ArrayList<>();
        for (MultiSearchResponse.Item item : multiSearchResponse) {
            ids.add(item.getResponse().getScrollId());
        }
        return ids;
    }
}
