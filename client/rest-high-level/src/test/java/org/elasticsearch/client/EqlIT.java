/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.elasticsearch.client.eql.EqlSearchResponse.Event;
import org.elasticsearch.client.eql.EqlStatsRequest;
import org.elasticsearch.client.eql.EqlStatsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EqlIT extends ESRestHighLevelClientTestCase {

    private static final String INDEX_NAME = "index";
    private static final int RECORD_COUNT = 40;
    private static final int DIVIDER = 4;

    @Before
    public void setup() throws Exception {
        setupRemoteClusterConfig("local_cluster");
        setupData();
    }

    private void setupData() throws IOException {
        final BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < RECORD_COUNT; i++) {
            final IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
            indexRequest.source(jsonBuilder()
                    .startObject()
                    .field("event_subtype_full", "already_running")
                    .startObject("event")
                    .field("category", "process")
                    .endObject()
                    .field("event_type", "foo")
                    .field("event_type_full", "process_event")
                    .field("opcode", ((i % DIVIDER) == 0) ? 1 : 0)
                    .field("pid", ((i % DIVIDER) == 0) ? 100 : 0)
                    .field("process_name", "System Idle Process")
                    .field("serial_event_id", i + 1)
                    .field("subtype", "create")
                    .field("@timestamp", String.format(Locale.ROOT, "2018-01-01T00:00:%02dZ", i))
                    .field("unique_pid", ((i % DIVIDER) == 0) ? 101 : 0)
                    .endObject());
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertEquals(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());

        RefreshResponse refreshResponse = highLevelClient().indices().refresh(new RefreshRequest(INDEX_NAME), RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());
    }

    private void assertResponse(EqlSearchResponse response, int count) {
        assertNotNull(response);
        assertFalse(response.isTimeout());
        assertNotNull(response.hits());
        assertNull(response.hits().sequences());
        assertNotNull(response.hits().events());
        assertThat(response.hits().events().size(), equalTo(count));
    }

    public void testBasicSearch() throws Exception {
        EqlClient eql = highLevelClient().eql();
        EqlSearchRequest request = new EqlSearchRequest("index", "process where true").size(RECORD_COUNT);
        assertResponse(execute(request, eql::search, eql::searchAsync), RECORD_COUNT);
    }

    @SuppressWarnings("unchecked")
    public void testSimpleConditionSearch() throws Exception {
        EqlClient eql = highLevelClient().eql();

        // test simple conditional
        EqlSearchRequest request = new EqlSearchRequest("index", "foo where pid > 0");

        // test with non-default event.category mapping
        request.eventCategoryField("event_type").size(RECORD_COUNT);

        EqlSearchResponse response = execute(request, eql::search, eql::searchAsync);
        assertResponse(response, RECORD_COUNT / DIVIDER);

        // test the content of the hits
        for (Event hit : response.hits().events()) {
            final Map<String, Object> source = hit.sourceAsMap();

            final Map<String, Object> event = (Map<String, Object>) source.get("event");
            assertThat(event.get("category"), equalTo("process"));
            assertThat(source.get("event_type"), equalTo("foo"));
            assertThat(source.get("event_type_full"), equalTo("process_event"));
            assertThat(source.get("opcode"), equalTo(1));
            assertThat(source.get("pid"), equalTo(100));
            assertThat(source.get("process_name"), equalTo("System Idle Process"));
            assertThat((int) source.get("serial_event_id"), greaterThan(0));
            assertThat(source.get("unique_pid"), equalTo(101));
        }
    }

    @SuppressWarnings("unchecked")
    public void testEqualsInFilterConditionSearch() throws Exception {
        EqlClient eql = highLevelClient().eql();

        EqlSearchRequest request = new EqlSearchRequest("index",
                "process where event_type_full == \"process_event\" and serial_event_id in (1,3,5)");

        EqlSearchResponse response = execute(request, eql::search, eql::searchAsync);
        assertResponse(response, 3);

        // test the content of the hits
        for (Event hit : response.hits().events()) {
            final Map<String, Object> source = hit.sourceAsMap();

            final Map<String, Object> event = (Map<String, Object>) source.get("event");
            assertThat(event.get("category"), equalTo("process"));
            assertThat(source.get("serial_event_id"), anyOf(equalTo(1), equalTo(3), equalTo(5)));
        }
    }

    public void testLargeMapping() throws Exception {
        final String index = "large_mapping_index";

        Request doc1 = new Request(HttpPut.METHOD_NAME, "/" + index + "/_doc/1");
        // use more exact fields (dates) than the default to verify that retrieval works and requesting doc values
        // would fail
        int PASS_DEFAULT_DOC_VALUES = IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.get(Settings.EMPTY) + 50;
        String now = DateUtils.nowWithMillisResolution().format(DateTimeFormatter.ISO_DATE_TIME);
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < PASS_DEFAULT_DOC_VALUES; i++) {
            sb.append("\"datetime" + i + "\":\"" + now + "\"");
            sb.append(",");
        }
        sb.append("\"event\": {\"category\": \"process\"},");
        sb.append("\"@timestamp\": \"2020-02-03T12:34:56Z\",");
        sb.append("\"serial_event_id\": 1");
        sb.append("}");
        doc1.setJsonEntity(sb.toString());

        client().performRequest(doc1);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));


        EqlClient eql = highLevelClient().eql();
        EqlSearchRequest request = new EqlSearchRequest(index, "process where true");
        EqlSearchResponse response = execute(request, eql::search, eql::searchAsync);
        assertNotNull(response);
        assertNotNull(response.hits());
        assertThat(response.hits().events().size(), equalTo(1));
    }

    // Basic test for stats
    // TODO: add more tests once the stats are hooked up
    public void testStats() throws Exception {
        EqlClient eql = highLevelClient().eql();
        EqlStatsRequest request = new EqlStatsRequest();
        EqlStatsResponse response = execute(request, eql::stats, eql::statsAsync);
        assertNotNull(response);
        assertNotNull(response.getHeader());
        assertThat(response.getHeader().getTotal(), greaterThan(0));
        assertThat(response.getNodes().size(), greaterThan(0));
    }
}
