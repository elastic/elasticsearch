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

package org.elasticsearch.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.get.GetActionIT.indexOrAlias;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGetActionIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testGetFieldsComplexField() throws Exception {
        assertAcked(prepareCreate("my-index")
                // multi types in 5.6
                .setSettings(Settings.builder().put("index.refresh_interval", -1).put("index.version.created", Version.V_5_6_0.id))
                .addMapping("my-type2", jsonBuilder().startObject().startObject("my-type2").startObject("properties")
                        .startObject("field1").field("type", "object").startObject("properties")
                        .startObject("field2").field("type", "object").startObject("properties")
                        .startObject("field3").field("type", "object").startObject("properties")
                        .startObject("field4").field("type", "text").field("store", true)
                        .endObject().endObject()
                        .endObject().endObject()
                        .endObject().endObject().endObject()
                        .endObject().endObject().endObject()));

        BytesReference source = BytesReference.bytes(jsonBuilder().startObject()
                .startArray("field1")
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value1")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value2")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endArray()
                .endObject());

        logger.info("indexing documents");

        client().prepareIndex("my-index", "my-type1", "1").setSource(source, XContentType.JSON).get();
        client().prepareIndex("my-index", "my-type2", "1").setSource(source, XContentType.JSON).get();

        logger.info("checking real time retrieval");

        String field = "field1.field2.field3.field4";
        GetResponse getResponse = client().prepareGet("my-index", "my-type1", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = client().prepareGet("my-index", "my-type2", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        logger.info("waiting for recoveries to complete");

        // Flush fails if shard has ongoing recoveries, make sure the cluster is settled down
        ensureGreen();

        logger.info("flushing");
        FlushResponse flushResponse = client().admin().indices().prepareFlush("my-index").setForce(true).get();
        if (flushResponse.getSuccessfulShards() == 0) {
            StringBuilder sb = new StringBuilder()
                    .append("failed to flush at least one shard. total shards [")
                    .append(flushResponse.getTotalShards())
                    .append("], failed shards: [")
                    .append(flushResponse.getFailedShards())
                    .append("]");
            for (DefaultShardOperationFailedException failure: flushResponse.getShardFailures()) {
                sb.append("\nShard failure: ").append(failure);
            }
            fail(sb.toString());
        }

        logger.info("checking post-flush retrieval");

        getResponse = client().prepareGet("my-index", "my-type1", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = client().prepareGet("my-index", "my-type2", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).isMetadataField(), equalTo(false));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));
    }

    public void testGetDocWithMultivaluedFieldsMultiTypeBWC() throws Exception {
        assertTrue("remove this multi type test", Version.CURRENT.before(Version.fromString("7.0.0")));
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                .startObject("field").field("type", "text").field("store", true).endObject()
                .endObject()
                .endObject().endObject());
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type2")
                .startObject("properties")
                .startObject("field").field("type", "text").field("store", true).endObject()
                .endObject()
                .endObject().endObject());
        assertAcked(prepareCreate("test")
                .addMapping("type1", mapping1, XContentType.JSON)
                .addMapping("type2", mapping2, XContentType.JSON)
                // multi types in 5.6
                .setSettings(Settings.builder().put("index.refresh_interval", -1).put("index.version.created", Version.V_5_6_0.id)));

        ensureGreen();

        GetResponse response = client().prepareGet("test", "type1", "1").get();
        assertThat(response.isExists(), equalTo(false));
        response = client().prepareGet("test", "type2", "1").get();
        assertThat(response.isExists(), equalTo(false));

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().array("field", "1", "2").endObject()).get();

        client().prepareIndex("test", "type2", "1")
                .setSource(jsonBuilder().startObject().array("field", "1", "2").endObject()).get();

        response = client().prepareGet("test", "type1", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getType(), equalTo("type1"));
        Set<String> fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));


        response = client().prepareGet("test", "type2", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getType(), equalTo("type2"));
        assertThat(response.getId(), equalTo("1"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));

        // Now test values being fetched from stored fields.
        refresh();
        response = client().prepareGet("test", "type1", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));

        response = client().prepareGet("test", "type2", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));
    }

    public void testGetFieldsMetaDataWithRouting() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("_doc", "field1", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
                .setSettings(
                        Settings.builder()
                                .put("index.refresh_interval", -1)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_6_0))); // multi-types in 5.6.0

        try (XContentBuilder source = jsonBuilder().startObject().field("field1", "value").endObject()) {
            client()
                    .prepareIndex("test", "_doc", "1")
                    .setRouting("1")
                    .setSource(source)
                    .get();
        }

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setRouting("1")
                    .setStoredFields("field1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }

        flush();

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "_doc", "1")
                    .setStoredFields("field1")
                    .setRouting("1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
        }
    }

    public void testGetFieldsMetaDataWithParentChild() throws Exception {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("my-type1", "_parent", "type=parent", "field1", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
                .setSettings(
                        Settings.builder()
                                .put("index.refresh_interval", -1)
                                .put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), Version.V_5_6_0))); // multi types in 5.6

        try (XContentBuilder source = jsonBuilder().startObject().field("field1", "value").endObject()) {
            client().prepareIndex("test", "my-type1", "1")
                    .setRouting("1")
                    .setParent("parent_1")
                    .setSource(source)
                    .get();
        }

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "my-type1", "1")
                    .setRouting("1")
                    .setStoredFields("field1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
            assertThat(getResponse.getField("_parent").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_parent").getValue().toString(), equalTo("parent_1"));
        }

        flush();

        {
            final GetResponse getResponse = client()
                    .prepareGet(indexOrAlias(), "my-type1", "1")
                    .setStoredFields("field1")
                    .setRouting("1")
                    .get();
            assertThat(getResponse.isExists(), equalTo(true));
            assertThat(getResponse.getField("field1").isMetadataField(), equalTo(false));
            assertThat(getResponse.getField("field1").getValue().toString(), equalTo("value"));
            assertThat(getResponse.getField("_routing").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_routing").getValue().toString(), equalTo("1"));
            assertThat(getResponse.getField("_parent").isMetadataField(), equalTo(true));
            assertThat(getResponse.getField("_parent").getValue().toString(), equalTo("parent_1"));
        }
    }

}
