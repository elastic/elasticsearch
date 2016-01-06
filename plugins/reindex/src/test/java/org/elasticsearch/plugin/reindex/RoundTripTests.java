/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.lucene.util.TestUtil.randomSimpleString;

/**
 * Round trip tests for all Streamable things declared in this plugin.
 */
public class RoundTripTests extends ESTestCase {
    public void testReindexRequest() throws IOException {
        ReindexRequest reindex = new ReindexRequest(new SearchRequest(), new IndexRequest());
        randomRequest(reindex);
        reindex.destination().version(randomFrom(Versions.MATCH_ANY, Versions.MATCH_DELETED, 12L, 1L, 123124L, 12L));
        reindex.destination().index("test");
        ReindexRequest tripped = new ReindexRequest();
        roundTrip(reindex, tripped);
        assertRequestEquals(reindex, tripped);
        assertEquals(reindex.destination().version(), tripped.destination().version());
        assertEquals(reindex.destination().index(), tripped.destination().index());
    }

    public void testUpdateByQueryRequest() throws IOException {
        UpdateByQueryRequest update = new UpdateByQueryRequest(new SearchRequest());
        randomRequest(update);
        UpdateByQueryRequest tripped = new UpdateByQueryRequest();
        roundTrip(update, tripped);
        assertRequestEquals(update, tripped);
    }

    public void testReindexResponse() throws IOException {
        ReindexResponse response = new ReindexResponse(randomPositiveLong(), randomPositiveLong(), randomPositiveLong(),
                randomPositiveInt(), randomPositiveLong(), randomPositiveLong(), randomFailures());
        ReindexResponse tripped = new ReindexResponse();
        roundTrip(response, tripped);
        assertResponseEquals(response, tripped);
        assertEquals(response.getCreated(), tripped.getCreated());
    }

    public void testBulkIndexByScrollResponse() throws IOException {
        BulkIndexByScrollResponse response = new BulkIndexByScrollResponse(randomPositiveLong(), randomPositiveLong(),
                randomPositiveInt(), randomPositiveLong(), randomPositiveLong(), randomFailures());
        BulkIndexByScrollResponse tripped = new BulkIndexByScrollResponse();
        roundTrip(response, tripped);
        assertResponseEquals(response, tripped);
    }

    private void randomRequest(AbstractBulkIndexByScrollRequest<?> request) {
        request.source().indices("test");
        request.source().source().size(between(1, 1000));
        request.size(random().nextBoolean() ? between(1, Integer.MAX_VALUE) : -1);
        request.abortOnVersionConflict(random().nextBoolean());
        request.refresh(rarely());
        request.timeout(TimeValue.parseTimeValue(randomTimeValue(), null, "test"));
        request.consistency(randomFrom(WriteConsistencyLevel.values()));
        request.script(random().nextBoolean() ? null : randomScript());
    }

    private void assertRequestEquals(AbstractBulkIndexByScrollRequest<?> request,
            AbstractBulkIndexByScrollRequest<?> tripped) {
        assertArrayEquals(request.source().indices(), tripped.source().indices());
        assertEquals(request.source().source().size(), tripped.source().source().size());
        assertEquals(request.abortOnVersionConflict(), tripped.abortOnVersionConflict());
        assertEquals(request.refresh(), tripped.refresh());
        assertEquals(request.timeout(), tripped.timeout());
        assertEquals(request.consistency(), tripped.consistency());
        assertEquals(request.script(), tripped.script());
    }

    private List<Failure> randomFailures() {
        return usually() ? emptyList()
                : singletonList(new Failure(randomSimpleString(random()), randomSimpleString(random()),
                        randomSimpleString(random()), new IllegalArgumentException("test")));
    }

    private void assertResponseEquals(BulkIndexByScrollResponse response, BulkIndexByScrollResponse tripped) {
        assertEquals(response.getTook(), tripped.getTook());
        assertEquals(response.getUpdated(), tripped.getUpdated());
        assertEquals(response.getBatches(), tripped.getBatches());
        assertEquals(response.getVersionConflicts(), tripped.getVersionConflicts());
        assertEquals(response.getNoops(), tripped.getNoops());
        assertEquals(response.getFailures().size(), tripped.getFailures().size());
        for (int i = 0; i < response.getFailures().size(); i++) {
            Failure expected = response.getFailures().get(i);
            Failure actual = tripped.getFailures().get(i);
            assertEquals(expected.getIndex(), actual.getIndex());
            assertEquals(expected.getType(), actual.getType());
            assertEquals(expected.getId(), actual.getId());
            assertEquals(expected.getMessage(), actual.getMessage());
            assertEquals(expected.getStatus(), actual.getStatus());
        }
    }

    private void roundTrip(Streamable example, Streamable empty) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        example.writeTo(out);
        empty.readFrom(out.bytes().streamInput());
    }

    private Script randomScript() {
        return new Script(randomSimpleString(random()), // Name
                randomFrom(ScriptType.values()), // Type
                random().nextBoolean() ? null : randomSimpleString(random()), // Language
                emptyMap()); // Params
    }

    private long randomPositiveLong() {
        long l;
        do {
            l = randomLong();
        } while (l < 0);
        return l;
    }

    private int randomPositiveInt() {
        return randomInt(Integer.MAX_VALUE);
    }
}
