/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class MatchOnlyTextMapperIT extends ESIntegTestCase {

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperExtrasPlugin.class);
    }

    public void testHighlightingWithMatchOnlyTextFieldMatchPhrase() throws IOException {

        // We index and retrieve a large number of documents to ensure that we go over multiple
        // segments, to ensure that the highlighter is using the correct segment lookups to
        // load the source.

        XContentBuilder mappings = jsonBuilder();
        mappings.startObject().startObject("properties").startObject("message").field("type", "match_only_text").endObject().endObject();
        mappings.endObject();
        assertAcked(prepareCreate("test").setMapping(mappings));
        BulkRequestBuilder bulk = client().prepareBulk("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 2000; i++) {
            bulk.add(
                client().prepareIndex()
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field(
                                "message",
                                "[.ds-.slm-history-7-2023.09.20-"
                                    + randomInt()
                                    + "][0] marking and sending shard failed due to [failed recovery]"
                            )
                            .endObject()
                    )
            );
        }
        BulkResponse bulkItemResponses = bulk.get();
        assertNoFailures(bulkItemResponses);

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(QueryBuilders.matchPhraseQuery("message", "marking and sending shard"))
                .setSize(500)
                .highlighter(new HighlightBuilder().field("message")),
            searchResponse -> {
                for (SearchHit searchHit : searchResponse.getHits()) {
                    assertThat(
                        searchHit.getHighlightFields().get("message").fragments()[0].string(),
                        containsString("<em>marking and sending shard</em>")
                    );
                }
            }
        );
    }

    public void testHighlightingWithMatchOnlyTextFieldSyntheticSource() throws IOException {

        // We index and retrieve a large number of documents to ensure that we go over multiple
        // segments, to ensure that the highlighter is using the correct segment lookups to
        // load the source.

        String mappings = """
            {
              "properties" : {
                "message" : { "type" : "match_only_text" }
              }
            }
            """;
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.mapping.source.mode", "synthetic");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(mappings));
        BulkRequestBuilder bulk = client().prepareBulk("test").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 2000; i++) {
            bulk.add(
                client().prepareIndex()
                    .setSource(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .field(
                                "message",
                                "[.ds-.slm-history-7-2023.09.20-"
                                    + randomInt()
                                    + "][0] marking and sending shard failed due to [failed recovery]"
                            )
                            .endObject()
                    )
            );
        }
        BulkResponse bulkItemResponses = bulk.get();
        assertNoFailures(bulkItemResponses);

        assertNoFailuresAndResponse(
            prepareSearch("test").setQuery(QueryBuilders.matchPhraseQuery("message", "marking and sending shard"))
                .setSize(500)
                .highlighter(new HighlightBuilder().field("message")),
            searchResponse -> {
                for (SearchHit searchHit : searchResponse.getHits()) {
                    assertThat(
                        searchHit.getHighlightFields().get("message").fragments()[0].string(),
                        containsString("<em>marking and sending shard</em>")
                    );
                }
            }
        );
    }

}
