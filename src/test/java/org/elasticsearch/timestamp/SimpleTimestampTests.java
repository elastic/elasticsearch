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

package org.elasticsearch.timestamp;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class SimpleTimestampTests  extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleTimestamp() throws Exception {

        client().admin().indices().prepareCreate("test")
                .addMapping("type1", jsonBuilder().startObject().startObject("type1").startObject("_timestamp").field("enabled", true).field("store", "yes").endObject().endObject().endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        logger.info("--> check with automatic timestamp");
        long now1 = System.currentTimeMillis();
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setRefresh(true).execute().actionGet();
        long now2 = System.currentTimeMillis();

        // we check both realtime get and non realtime get
        GetResponse getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(true).execute().actionGet();
        long timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, greaterThanOrEqualTo(now1));
        assertThat(timestamp, lessThanOrEqualTo(now2));
        // verify its the same timestamp when going the replica
        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(true).execute().actionGet();
        assertThat(((Number) getResponse.getField("_timestamp").getValue()).longValue(), equalTo(timestamp));

        // non realtime get (stored)
        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, greaterThanOrEqualTo(now1));
        assertThat(timestamp, lessThanOrEqualTo(now2));
        // verify its the same timestamp when going the replica
        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.getField("_timestamp").getValue()).longValue(), equalTo(timestamp));

        logger.info("--> check with custom timestamp (numeric)");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimestamp("10").setRefresh(true).execute().actionGet();

        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, equalTo(10l));
        // verify its the same timestamp when going the replica
        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.getField("_timestamp").getValue()).longValue(), equalTo(timestamp));

        logger.info("--> check with custom timestamp (string)");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").setTimestamp("1970-01-01T00:00:00.020").setRefresh(true).execute().actionGet();

        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        timestamp = ((Number) getResponse.getField("_timestamp").getValue()).longValue();
        assertThat(timestamp, equalTo(20l));
        // verify its the same timestamp when going the replica
        getResponse = client().prepareGet("test", "type1", "1").setFields("_timestamp").setRealtime(false).execute().actionGet();
        assertThat(((Number) getResponse.getField("_timestamp").getValue()).longValue(), equalTo(timestamp));
    }

    @Test // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveTimestampConfiguration() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_timestamp").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertTimestampMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("properties").startObject("otherField").field("type", "string").endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure timestamp field is still in mapping
        assertTimestampMappingEnabled(index, type, true);
    }

    @Test
    public void testThatTimestampCanBeSwitchedOnAndOff() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_timestamp").field("enabled", true).field("store", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertTimestampMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("_timestamp").field("enabled", false).field("store", true).endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure timestamp field is still in mapping
        assertTimestampMappingEnabled(index, type, false);
    }

    private void assertTimestampMappingEnabled(String index, String type, boolean enabled) {
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).addTypes(type).get();
        MappingMetaData.Timestamp timestamp = getMappingsResponse.getMappings().get(index).get(type).timestamp();
        assertThat(timestamp, is(notNullValue()));
        String errMsg = String.format(Locale.ROOT, "Expected timestamp field mapping to be "+ (enabled ? "enabled" : "disabled") +" for %s/%s", index, type);
        assertThat(errMsg, timestamp.enabled(), is(enabled));
    }
}
