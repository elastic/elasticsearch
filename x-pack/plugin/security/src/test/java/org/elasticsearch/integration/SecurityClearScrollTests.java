/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SecurityClearScrollTests extends SecurityIntegTestCase {

    private List<String> scrollIds;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(new SecureString("change_me".toCharArray())));
        return super.configUsers() +
            "allowed_user:" + usersPasswdHashed + "\n" +
            "denied_user:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
            "allowed_role:allowed_user\n" +
            "denied_role:denied_user\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
            "\nallowed_role:\n" +
            "  cluster:\n" +
            "    - cluster:admin/indices/scroll/clear_all \n" +
            "denied_role:\n" +
            "  indices:\n" +
            "    - names: '*'\n" +
            "      privileges: [ALL]\n";
    }

    @Before
    public void indexRandomDocuments() {
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(IMMEDIATE);
        for (int i = 0; i < randomIntBetween(10, 50); i++) {
            bulkRequestBuilder.add(client().prepareIndex("index")
                .setId(String.valueOf(i)).setSource("{ \"foo\" : \"bar\" }", XContentType.JSON));
        }
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        assertThat(bulkItemResponses.hasFailures(), is(false));

        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        int count = randomIntBetween(5, 15);
        for (int i = 0; i < count; i++) {
            multiSearchRequestBuilder.add(client().prepareSearch("index").setScroll("10m").setSize(1));
        }
        MultiSearchResponse multiSearchResponse = multiSearchRequestBuilder.get();
        scrollIds = getScrollIds(multiSearchResponse);
    }

    @After
    public void clearScrolls() {
        //clear all scroll ids from the default admin user, just in case any of test fails
        client().prepareClearScroll().addScrollId("_all").get();
    }

    public void testThatClearingAllScrollIdsWorks() throws Exception {
        String user = "allowed_user:change_me";
        String basicAuth = basicAuthHeaderValue("allowed_user", new SecureString("change_me".toCharArray()));
        Map<String, String> headers = new HashMap<>();
        headers.put(SecurityField.USER_SETTING.getKey(), user);
        headers.put(BASIC_AUTH_HEADER, basicAuth);
        ClearScrollResponse clearScrollResponse = client().filterWithHeader(headers)
            .prepareClearScroll()
            .addScrollId("_all").get();
        assertThat(clearScrollResponse.isSucceeded(), is(true));

        assertThatScrollIdsDoNotExist(scrollIds);
    }

    public void testThatClearingAllScrollIdsRequirePermissions() throws Exception {
        String user = "denied_user:change_me";
        String basicAuth = basicAuthHeaderValue("denied_user", new SecureString("change_me".toCharArray()));
        Map<String, String> headers = new HashMap<>();
        headers.put(SecurityField.USER_SETTING.getKey(), user);
        headers.put(BASIC_AUTH_HEADER, basicAuth);
        assertThrows(client().filterWithHeader(headers)
                .prepareClearScroll()
                .addScrollId("_all"), ElasticsearchSecurityException.class,
                "action [cluster:admin/indices/scroll/clear_all] is unauthorized for user [denied_user]");

        // deletion of scroll ids should work
        ClearScrollResponse clearByIdScrollResponse = client().prepareClearScroll().setScrollIds(scrollIds).get();
        assertThat(clearByIdScrollResponse.isSucceeded(), is(true));

        // test with each id, that they do not exist
        assertThatScrollIdsDoNotExist(scrollIds);
    }

    private void assertThatScrollIdsDoNotExist(List<String> scrollIds) {
        for (String scrollId : scrollIds) {
            SearchPhaseExecutionException expectedException =
                    expectThrows(SearchPhaseExecutionException.class, () -> client().prepareSearchScroll(scrollId).get());
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
