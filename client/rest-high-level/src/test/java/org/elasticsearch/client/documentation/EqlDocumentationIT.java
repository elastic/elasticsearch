/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.documentation;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Event;
import org.elasticsearch.client.eql.EqlSearchResponse.Hits;
import org.elasticsearch.client.eql.EqlSearchResponse.Sequence;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * Documentation for EQL APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
@SuppressWarnings("removal")
public class EqlDocumentationIT extends ESRestHighLevelClientTestCase {

    @Before
    void setUpIndex() throws IOException {
        String index = "my-index";
        CreateIndexResponse createIndexResponse = highLevelClient().indices().create(new CreateIndexRequest(index), RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());
        BulkRequest bulk = new BulkRequest(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(new IndexRequest().source(JSON, "event_category", "process", "timestamp", "2021-11-23T00:00:00Z", "tie", 1, "host", "A"));
        bulk.add(new IndexRequest().source(JSON, "event_category", "process", "timestamp", "2021-11-23T00:00:00Z", "tie", 2, "host", "B"));
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
    }

    public void testEqlSearch() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::eql-search-request
        String indices = "my-index"; // <1>
        String query = "any where true"; // <2>
        EqlSearchRequest request = new EqlSearchRequest(indices, query);
        // end::eql-search-request

        // tag::eql-search-request-arguments
        request.eventCategoryField("event_category"); // <1>
        request.fetchSize(50); // <2>
        request.size(15); // <3>
        request.tiebreakerField("tie"); // <4>
        request.timestampField("timestamp"); // <5>
        request.filter(QueryBuilders.matchAllQuery()); // <6>
        request.resultPosition("head"); // <7>

        List<FieldAndFormat> fields = new ArrayList<>();
        fields.add(new FieldAndFormat("hostname", null));
        request.fetchFields(fields); // <8>

        IndicesOptions op = IndicesOptions.fromOptions(true, true, true, false);
        request.indicesOptions(op); // <9>

        Map<String, Object> settings = new HashMap<>();
        settings.put("type", "keyword");
        settings.put("script", "emit(doc['host.keyword'].value)");
        Map<String, Object> field = new HashMap<>();
        field.put("hostname", settings);
        request.runtimeMappings(field); // <10>

        request.waitForCompletionTimeout(TimeValue.timeValueMinutes(1)); // <11>
        request.keepOnCompletion(true); // <12>
        request.keepAlive(TimeValue.timeValueHours(12)); // <13>
        // end::eql-search-request-arguments

        // Ignore warning about ignore_throttled being deprecated
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build();
        // tag::eql-search-response
        EqlSearchResponse response = client.eql().search(request, options);
        response.id(); // <1>
        response.isPartial(); // <2>
        response.isRunning(); // <3>
        response.isTimeout(); // <4>
        response.took(); // <5>
        Hits hits = response.hits(); // <6>
        hits.totalHits(); // <7>
        List<Event> events = hits.events(); // <8>
        List<Sequence> sequences = hits.sequences(); // <9>
        Map<String, Object> event = events.get(0).sourceAsMap();
        Map<String, DocumentField> fetchField = events.get(0).fetchFields();
        fetchField.get("hostname").getValues(); // <10>
        // end::eql-search-response
        assertFalse(response.isPartial());
        assertFalse(response.isRunning());
        assertFalse(response.isTimeout());
        assertEquals(2, hits.totalHits().value);
        assertEquals(2, events.size());
        assertNull(sequences);
        assertEquals(1, fetchField.size());
        assertEquals(1, fetchField.get("hostname").getValues().size());
        assertEquals("A", fetchField.get("hostname").getValues().get(0));
    }
}
