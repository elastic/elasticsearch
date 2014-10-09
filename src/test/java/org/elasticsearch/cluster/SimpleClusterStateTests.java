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

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.CollectionAssertions;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateExists;
import static org.hamcrest.Matchers.*;

/**
 * Checking simple filtering capabilites of the cluster state
 *
 */
public class SimpleClusterStateTests extends ElasticsearchIntegrationTest {

    @Before
    public void indexData() throws Exception {
        index("foo", "bar", "1", XContentFactory.jsonBuilder().startObject().field("foo", "foo").endObject());
        index("fuu", "buu", "1", XContentFactory.jsonBuilder().startObject().field("fuu", "fuu").endObject());
        index("baz", "baz", "1", XContentFactory.jsonBuilder().startObject().field("baz", "baz").endObject());
        refresh();
    }

    @Test
    public void testRoutingTable() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setRoutingTable(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("foo"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("fuu"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("baz"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("non-existent"), is(false));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("foo"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("fuu"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("baz"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("non-existent"), is(false));
    }

    @Test
    public void testNodes() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        assertThat(clusterStateResponse.getState().nodes().nodes().size(), is(cluster().size()));

        ClusterStateResponse clusterStateResponseFiltered = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponseFiltered.getState().nodes().nodes().size(), is(0));
    }

    @Test
    public void testMetadata() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setMetaData(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().metaData().indices().size(), is(3));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().metaData().indices().size(), is(0));
    }

    @Test
    public void testIndexTemplates() throws Exception {
        client().admin().indices().preparePutTemplate("foo_template")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "string").field("store", "yes").endObject()
                        .startObject("field2").field("type", "string").field("store", "yes").field("index", "not_analyzed").endObject()
                        .endObject().endObject().endObject())
                .get();

        client().admin().indices().preparePutTemplate("fuu_template")
                .setTemplate("test*")
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                .get();

        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponseUnfiltered.getState().metaData().templates().size(), is(greaterThanOrEqualTo(2)));

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates("foo_template").get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "foo_template");
    }

    @Test
    public void testThatFilteringByIndexWorksForMetadataAndRoutingTable() throws Exception {
        ClusterStateResponse clusterStateResponseFiltered = client().admin().cluster().prepareState().clear()
                .setMetaData(true).setRoutingTable(true).setIndices("foo", "fuu", "non-existent").get();

        // metadata
        assertThat(clusterStateResponseFiltered.getState().metaData().indices().size(), is(2));
        assertThat(clusterStateResponseFiltered.getState().metaData().indices(), CollectionAssertions.hasKey("foo"));
        assertThat(clusterStateResponseFiltered.getState().metaData().indices(), CollectionAssertions.hasKey("fuu"));

        // routing table
        assertThat(clusterStateResponseFiltered.getState().routingTable().hasIndex("foo"), is(true));
        assertThat(clusterStateResponseFiltered.getState().routingTable().hasIndex("fuu"), is(true));
        assertThat(clusterStateResponseFiltered.getState().routingTable().hasIndex("baz"), is(false));
    }

    @Test
    public void testLargeClusterStatePublishing() throws Exception {
        int estimatedBytesSize = scaledRandomIntBetween(ByteSizeValue.parseBytesSizeValue("10k").bytesAsInt(), ByteSizeValue.parseBytesSizeValue("256k").bytesAsInt());
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        int counter = 0;
        int numberOfFields = 0;
        while (true) {
            mapping.startObject(Strings.randomBase64UUID()).field("type", "string").endObject();
            counter += 10; // each field is about 10 bytes, assuming compression in place
            numberOfFields++;
            if (counter > estimatedBytesSize) {
                break;
            }
        }
        logger.info("number of fields [{}], estimated bytes [{}]", numberOfFields, estimatedBytesSize);
        mapping.endObject().endObject().endObject();

        int numberOfShards = scaledRandomIntBetween(1, cluster().numDataNodes());
        // if the create index is ack'ed, then all nodes have successfully processed the cluster state
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .addMapping("type", mapping)
                .setTimeout("60s").get());
        ensureGreen(); // wait for green state, so its both green, and there are no more pending events
        MappingMetaData masterMappingMetaData = client().admin().indices().prepareGetMappings("test").setTypes("type").get().getMappings().get("test").get("type");
        for (Client client : clients()) {
            MappingMetaData mappingMetadata = client.admin().indices().prepareGetMappings("test").setTypes("type").setLocal(true).get().getMappings().get("test").get("type");
            assertThat(mappingMetadata.source().string(), equalTo(masterMappingMetaData.source().string()));
            assertThat(mappingMetadata, equalTo(masterMappingMetaData));
        }
    }
}
