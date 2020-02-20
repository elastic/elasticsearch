/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AliasWithClosedIndexTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    // https://github.com/elastic/elasticsearch/issues/29948
    public void testCatShardsWithAliasPointingToClosedIndex() throws Exception {
        createIndex("test");
        assertAcked(client().admin().indices().prepareAliases().addAlias("test", "test-alias").get());
        assertAcked(client().admin().indices().prepareClose("test").get());

        final RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
            new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        final RequestOptions options = optionsBuilder.build();
        final Request request = new Request("GET", "/_cat/shards");
        request.setOptions(options);
        assertThat(getRestClient().performRequest(request).getStatusLine().getStatusCode(), is(200));
    }

    // https://github.com/elastic/elasticsearch/issues/32238
    public void testSearchWithAliasPointingToClosedIndex() throws Exception {
        createIndex("test1", "test2");
        assertAcked(client().admin().indices().prepareAliases()
            .addAlias("test1", "test-alias")
            .addAlias("test2", "test-alias")
            .get());
        index("test1", "1", Map.of("test", "foo"));
        index("test2", "1", Map.of("test", "bar"));

        refresh("test1", "test2");

        SearchResponse response = client().prepareSearch()
            .setSource(SearchSourceBuilder.searchSource().size(0)
                .aggregation(AggregationBuilders.terms("indices").field("_index").size(200)))
            .get();
        assertThat(response.getAggregations().asList().size(), is(1));

        assertAcked(client().admin().indices().prepareClose(randomFrom("test1", "test2")).get());
        response = client().prepareSearch()
            .setSource(SearchSourceBuilder.searchSource().size(0)
                .aggregation(AggregationBuilders.terms("indices").field("_index").size(200)))
            .get();
        assertThat(response.getAggregations().asList().size(), is(1));

        assertAcked(client().admin().indices().prepareClose("test1", "test2").get());
        response = client().prepareSearch()
            .setSource(SearchSourceBuilder.searchSource().size(0)
                .aggregation(AggregationBuilders.terms("indices").field("_index").size(200)))
            .get();
        assertThat(response.getAggregations(), nullValue());
    }
}
