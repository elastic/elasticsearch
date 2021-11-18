/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.termvectors;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TermVectorsServiceTests extends ESSingleNodeTestCase {

    public void testTook() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("test", Settings.EMPTY, mapping);
        ensureGreen();

        client().prepareIndex("test").setId("0").setSource("field", "foo bar").setRefreshPolicy(IMMEDIATE).get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());

        List<Long> longs = Stream.of(abs(randomLong()), abs(randomLong())).sorted().collect(toList());

        TermVectorsRequest request = new TermVectorsRequest("test", "0");
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request, longs.iterator()::next);

        assertThat(response, notNullValue());
        assertThat(response.getTook().getMillis(), equalTo(TimeUnit.NANOSECONDS.toMillis(longs.get(1) - longs.get(0))));
    }

    public void testDocFreqs() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder().put("number_of_shards", 1).build();
        createIndex("test", settings, mapping);
        ensureGreen();

        int max = between(3, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < max; i++) {
            bulk.add(
                client().prepareIndex("test").setId(Integer.toString(i)).setSource("text", "the quick brown fox jumped over the lazy dog")
            );
        }
        bulk.get();

        TermVectorsRequest request = new TermVectorsRequest("test", "0").termStatistics(true);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request);
        assertEquals(1, response.getFields().size());

        Terms terms = response.getFields().terms("text");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            assertEquals(max, iterator.docFreq());
        }
    }

    public void testWithIndexedPhrases() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .field("index_phrases", true)
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder().put("number_of_shards", 1).build();
        createIndex("test", settings, mapping);
        ensureGreen();

        int max = between(3, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < max; i++) {
            bulk.add(
                client().prepareIndex("test").setId(Integer.toString(i)).setSource("text", "the quick brown fox jumped over the lazy dog")
            );
        }
        bulk.get();

        TermVectorsRequest request = new TermVectorsRequest("test", "0").termStatistics(true);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request);
        assertEquals(2, response.getFields().size());

        Terms terms = response.getFields().terms("text");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            assertEquals(max, iterator.docFreq());
        }

        Terms phrases = response.getFields().terms("text._index_phrase");
        TermsEnum phraseIterator = phrases.iterator();
        while (phraseIterator.next() != null) {
            assertEquals(max, phraseIterator.docFreq());
        }
    }
}
