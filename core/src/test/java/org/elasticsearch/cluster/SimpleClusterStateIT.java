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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.CollectionAssertions;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateExists;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

/**
 * Checking simple filtering capabilities of the cluster state
 *
 */
public class SimpleClusterStateIT extends ESIntegTestCase {

    @Before
    public void indexData() throws Exception {
        index("foo", "bar", "1", XContentFactory.jsonBuilder().startObject().field("foo", "foo").endObject());
        index("fuu", "buu", "1", XContentFactory.jsonBuilder().startObject().field("fuu", "fuu").endObject());
        index("baz", "baz", "1", XContentFactory.jsonBuilder().startObject().field("baz", "baz").endObject());
        refresh();
    }

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

    public void testNodes() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setNodes(true).get();
        assertThat(clusterStateResponse.getState().nodes().getNodes().size(), is(cluster().size()));

        ClusterStateResponse clusterStateResponseFiltered = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponseFiltered.getState().nodes().getNodes().size(), is(0));
    }

    public void testMetadata() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().clear().setMetaData(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().metaData().indices().size(), is(3));

        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().metaData().indices().size(), is(0));
    }

    public void testIndexTemplates() throws Exception {
        client().admin().indices().preparePutTemplate("foo_template")
                .setTemplate("te*")
                .setOrder(0)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field1").field("type", "text").field("store", true).endObject()
                        .startObject("field2").field("type", "keyword").field("store", true).endObject()
                        .endObject().endObject().endObject())
                .get();

        client().admin().indices().preparePutTemplate("fuu_template")
                .setTemplate("test*")
                .setOrder(1)
                .addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                        .startObject("field2").field("type", "text").field("store", "no").endObject()
                        .endObject().endObject().endObject())
                .get();

        ClusterStateResponse clusterStateResponseUnfiltered = client().admin().cluster().prepareState().get();
        assertThat(clusterStateResponseUnfiltered.getState().metaData().templates().size(), is(greaterThanOrEqualTo(2)));

        GetIndexTemplatesResponse getIndexTemplatesResponse = client().admin().indices().prepareGetTemplates("foo_template").get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "foo_template");
    }

    public void testThatFilteringByIndexWorksForMetadataAndRoutingTable() throws Exception {
        testFilteringByIndexWorks(new String[]{"foo", "fuu", "non-existent"}, new String[]{"foo", "fuu"});
        testFilteringByIndexWorks(new String[]{"baz"}, new String[]{"baz"});
        testFilteringByIndexWorks(new String[]{"f*"}, new String[]{"foo", "fuu"});
        testFilteringByIndexWorks(new String[]{"b*"}, new String[]{"baz"});
        testFilteringByIndexWorks(new String[]{"*u"}, new String[]{"fuu"});

        String[] randomIndices = randomFrom(new String[]{"*"}, new String[]{MetaData.ALL}, Strings.EMPTY_ARRAY, new String[]{"f*", "b*"});
        testFilteringByIndexWorks(randomIndices, new String[]{"foo", "fuu", "baz"});
    }

    /**
     * Retrieves the cluster state for the given indices and then checks
     * that the cluster state returns coherent data for both routing table and metadata.
     */
    private void testFilteringByIndexWorks(String[] indices, String[] expected) {
        ClusterStateResponse clusterState = client().admin().cluster().prepareState()
                                                                            .clear()
                                                                            .setMetaData(true)
                                                                            .setRoutingTable(true)
                                                                            .setIndices(indices)
                                                                            .get();

        ImmutableOpenMap<String, IndexMetaData> metaData = clusterState.getState().getMetaData().indices();
        assertThat(metaData.size(), is(expected.length));

        RoutingTable routingTable = clusterState.getState().getRoutingTable();
        assertThat(routingTable.indicesRouting().size(), is(expected.length));

        for (String expectedIndex : expected) {
            assertThat(metaData, CollectionAssertions.hasKey(expectedIndex));
            assertThat(routingTable.hasIndex(expectedIndex), is(true));
        }
    }

    public void testLargeClusterStatePublishing() throws Exception {
        int estimatedBytesSize = scaledRandomIntBetween(ByteSizeValue.parseBytesSizeValue("10k", "estimatedBytesSize").bytesAsInt(),
                                                        ByteSizeValue.parseBytesSizeValue("256k", "estimatedBytesSize").bytesAsInt());
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        int counter = 0;
        int numberOfFields = 0;
        while (true) {
            mapping.startObject(UUIDs.randomBase64UUID()).field("type", "text").endObject();
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
                .setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards,
                        IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0,
                        MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
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

    public void testIndicesOptions() throws Exception {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("f*")
                .get();
        assertThat(clusterStateResponse.getState().metaData().indices().size(), is(2));

        // close one index
        client().admin().indices().close(Requests.closeIndexRequest("fuu")).get();
        clusterStateResponse = client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("f*").get();
        assertThat(clusterStateResponse.getState().metaData().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metaData().index("foo").getState(), equalTo(IndexMetaData.State.OPEN));

        // expand_wildcards_closed should toggle return only closed index fuu
        IndicesOptions expandCloseOptions = IndicesOptions.fromOptions(false, true, false, true);
        clusterStateResponse = client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("f*")
                .setIndicesOptions(expandCloseOptions).get();
        assertThat(clusterStateResponse.getState().metaData().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metaData().index("fuu").getState(), equalTo(IndexMetaData.State.CLOSE));

        // ignore_unavailable set to true should not raise exception on fzzbzz
        IndicesOptions ignoreUnavailabe = IndicesOptions.fromOptions(true, true, true, false);
        clusterStateResponse = client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("fzzbzz")
                .setIndicesOptions(ignoreUnavailabe).get();
        assertThat(clusterStateResponse.getState().metaData().indices().isEmpty(), is(true));

        // empty wildcard expansion result should work when allowNoIndices is
        // turned on
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        clusterStateResponse = client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("a*")
                .setIndicesOptions(allowNoIndices).get();
        assertThat(clusterStateResponse.getState().metaData().indices().isEmpty(), is(true));
    }

    public void testIndicesOptionsOnAllowNoIndicesFalse() throws Exception {
        // empty wildcard expansion throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, false, true, false);
        try {
            client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("a*").setIndicesOptions(allowNoIndices).get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }

    public void testIndicesIgnoreUnavailableFalse() throws Exception {
        // ignore_unavailable set to false throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        try {
            client().admin().cluster().prepareState().clear().setMetaData(true).setIndices("fzzbzz").setIndicesOptions(allowNoIndices).get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index"));
        }
    }
}
