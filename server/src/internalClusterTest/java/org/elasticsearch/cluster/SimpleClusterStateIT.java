/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertIndexTemplateExists;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

/**
 * Checking simple filtering capabilities of the cluster state
 *
 */
public class SimpleClusterStateIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(PrivateCustomPlugin.class);
    }

    @Before
    public void indexData() throws Exception {
        index("foo", "1", XContentFactory.jsonBuilder().startObject().field("foo", "foo").endObject());
        index("fuu", "1", XContentFactory.jsonBuilder().startObject().field("fuu", "fuu").endObject());
        index("baz", "1", XContentFactory.jsonBuilder().startObject().field("baz", "baz").endObject());
        refresh();
    }

    public void testRoutingTable() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = clusterAdmin().prepareState().clear().setRoutingTable(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("foo"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("fuu"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("baz"), is(true));
        assertThat(clusterStateResponseUnfiltered.getState().routingTable().hasIndex("non-existent"), is(false));

        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("foo"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("fuu"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("baz"), is(false));
        assertThat(clusterStateResponse.getState().routingTable().hasIndex("non-existent"), is(false));
    }

    public void testNodes() throws Exception {
        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().clear().setNodes(true).get();
        assertThat(clusterStateResponse.getState().nodes().getNodes().size(), is(cluster().size()));

        ClusterStateResponse clusterStateResponseFiltered = clusterAdmin().prepareState().clear().get();
        assertThat(clusterStateResponseFiltered.getState().nodes().getNodes().size(), is(0));
    }

    public void testMetadata() throws Exception {
        ClusterStateResponse clusterStateResponseUnfiltered = clusterAdmin().prepareState().clear().setMetadata(true).get();
        assertThat(clusterStateResponseUnfiltered.getState().metadata().indices().size(), is(3));

        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().clear().get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(0));
    }

    public void testMetadataVersion() {
        createIndex("index-1");
        createIndex("index-2");
        long baselineVersion = clusterAdmin().prepareState().get().getState().metadata().version();
        assertThat(baselineVersion, greaterThan(0L));
        assertThat(
            clusterAdmin().prepareState().setIndices("index-1").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            clusterAdmin().prepareState().setIndices("index-2").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            clusterAdmin().prepareState().setIndices("*").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(
            clusterAdmin().prepareState().setIndices("not-found").get().getState().metadata().version(),
            greaterThanOrEqualTo(baselineVersion)
        );
        assertThat(clusterAdmin().prepareState().clear().setMetadata(false).get().getState().metadata().version(), equalTo(0L));
    }

    public void testIndexTemplates() throws Exception {
        indicesAdmin().preparePutTemplate("foo_template")
            .setPatterns(Collections.singletonList("te*"))
            .setOrder(0)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "text")
                    .field("store", true)
                    .endObject()
                    .startObject("field2")
                    .field("type", "keyword")
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        indicesAdmin().preparePutTemplate("fuu_template")
            .setPatterns(Collections.singletonList("test*"))
            .setOrder(1)
            .setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field2")
                    .field("type", "text")
                    .field("store", false)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        ClusterStateResponse clusterStateResponseUnfiltered = clusterAdmin().prepareState().get();
        assertThat(clusterStateResponseUnfiltered.getState().metadata().templates().size(), is(greaterThanOrEqualTo(2)));

        GetIndexTemplatesResponse getIndexTemplatesResponse = indicesAdmin().prepareGetTemplates("foo_template").get();
        assertIndexTemplateExists(getIndexTemplatesResponse, "foo_template");
    }

    public void testThatFilteringByIndexWorksForMetadataAndRoutingTable() throws Exception {
        testFilteringByIndexWorks(new String[] { "foo", "fuu", "non-existent" }, new String[] { "foo", "fuu" });
        testFilteringByIndexWorks(new String[] { "baz" }, new String[] { "baz" });
        testFilteringByIndexWorks(new String[] { "f*" }, new String[] { "foo", "fuu" });
        testFilteringByIndexWorks(new String[] { "b*" }, new String[] { "baz" });
        testFilteringByIndexWorks(new String[] { "*u" }, new String[] { "fuu" });

        String[] randomIndices = randomFrom(
            new String[] { "*" },
            new String[] { Metadata.ALL },
            Strings.EMPTY_ARRAY,
            new String[] { "f*", "b*" }
        );
        testFilteringByIndexWorks(randomIndices, new String[] { "foo", "fuu", "baz" });
    }

    /**
     * Retrieves the cluster state for the given indices and then checks
     * that the cluster state returns coherent data for both routing table and metadata.
     */
    private void testFilteringByIndexWorks(String[] indices, String[] expected) {
        ClusterStateResponse clusterState = clusterAdmin().prepareState()
            .clear()
            .setMetadata(true)
            .setRoutingTable(true)
            .setIndices(indices)
            .get();

        Map<String, IndexMetadata> metadata = clusterState.getState().getMetadata().indices();
        assertThat(metadata.size(), is(expected.length));

        RoutingTable routingTable = clusterState.getState().getRoutingTable();
        assertThat(routingTable.indicesRouting().size(), is(expected.length));

        for (String expectedIndex : expected) {
            assertThat(metadata, hasKey(expectedIndex));
            assertThat(routingTable.hasIndex(expectedIndex), is(true));
        }
    }

    public void testLargeClusterStatePublishing() throws Exception {
        int estimatedBytesSize = scaledRandomIntBetween(
            ByteSizeValue.parseBytesSizeValue("10k", "estimatedBytesSize").bytesAsInt(),
            ByteSizeValue.parseBytesSizeValue("256k", "estimatedBytesSize").bytesAsInt()
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        int counter = 0;
        int numberOfFields = 0;
        while (true) {
            mapping.startObject(UUIDs.randomBase64UUID()).field("type", "boolean").endObject();
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
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(
                    indexSettings(numberOfShards, 0).put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                )
                .setMapping(mapping)
                .setTimeout("60s")
        );
        ensureGreen(); // wait for green state, so its both green, and there are no more pending events
        MappingMetadata masterMappingMetadata = indicesAdmin().prepareGetMappings("test").get().getMappings().get("test");
        for (Client client : clients()) {
            MappingMetadata mappingMetadata = client.admin()
                .indices()
                .prepareGetMappings("test")
                .setLocal(true)
                .get()
                .getMappings()
                .get("test");
            assertThat(mappingMetadata.source().string(), equalTo(masterMappingMetadata.source().string()));
            assertThat(mappingMetadata, equalTo(masterMappingMetadata));
        }
    }

    public void testIndicesOptions() throws Exception {
        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().clear().setMetadata(true).setIndices("f*").get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(2));
        ensureGreen("fuu");

        // close one index
        assertAcked(indicesAdmin().close(new CloseIndexRequest("fuu")).get());
        clusterStateResponse = clusterAdmin().prepareState().clear().setMetadata(true).setIndices("f*").get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metadata().index("foo").getState(), equalTo(IndexMetadata.State.OPEN));

        // expand_wildcards_closed should toggle return only closed index fuu
        IndicesOptions expandCloseOptions = IndicesOptions.fromOptions(false, true, false, true);
        clusterStateResponse = clusterAdmin().prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("f*")
            .setIndicesOptions(expandCloseOptions)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().size(), is(1));
        assertThat(clusterStateResponse.getState().metadata().index("fuu").getState(), equalTo(IndexMetadata.State.CLOSE));

        // ignore_unavailable set to true should not raise exception on fzzbzz
        IndicesOptions ignoreUnavailabe = IndicesOptions.fromOptions(true, true, true, false);
        clusterStateResponse = clusterAdmin().prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("fzzbzz")
            .setIndicesOptions(ignoreUnavailabe)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().isEmpty(), is(true));

        // empty wildcard expansion result should work when allowNoIndices is
        // turned on
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        clusterStateResponse = clusterAdmin().prepareState()
            .clear()
            .setMetadata(true)
            .setIndices("a*")
            .setIndicesOptions(allowNoIndices)
            .get();
        assertThat(clusterStateResponse.getState().metadata().indices().isEmpty(), is(true));
    }

    public void testIndicesOptionsOnAllowNoIndicesFalse() throws Exception {
        // empty wildcard expansion throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, false, true, false);
        try {
            clusterAdmin().prepareState().clear().setMetadata(true).setIndices("a*").setIndicesOptions(allowNoIndices).get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [a*]"));
        }
    }

    public void testIndicesIgnoreUnavailableFalse() throws Exception {
        // ignore_unavailable set to false throws exception when allowNoIndices is turned off
        IndicesOptions allowNoIndices = IndicesOptions.fromOptions(false, true, true, false);
        try {
            clusterAdmin().prepareState().clear().setMetadata(true).setIndices("fzzbzz").setIndicesOptions(allowNoIndices).get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [fzzbzz]"));
        }
    }

    public void testPrivateCustomsAreExcluded() throws Exception {
        // ensure that the custom is injected into the cluster state
        assertBusy(() -> assertTrue(clusterService().state().customs().containsKey("test")));
        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().setCustoms(true).get();
        assertFalse(clusterStateResponse.getState().customs().containsKey("test"));
    }

    private static class TestCustom extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

        private final int value;

        TestCustom(int value) {
            this.value = value;
        }

        TestCustom(StreamInput in) throws IOException {
            this.value = in.readInt();
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        static NamedDiff<ClusterState.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(ClusterState.Custom.class, "test", in);
        }

        @Override
        public boolean isPrivate() {
            return true;
        }
    }

    public static class PrivateCustomPlugin extends Plugin implements ClusterPlugin {

        public PrivateCustomPlugin() {}

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, "test", TestCustom::new));
            entries.add(new NamedWriteableRegistry.Entry(NamedDiff.class, "test", TestCustom::readDiffFrom));
            return entries;
        }

        private final AtomicBoolean installed = new AtomicBoolean();

        @Override
        public Collection<?> createComponents(PluginServices services) {
            ClusterService clusterService = services.clusterService();

            clusterService.addListener(event -> {
                final ClusterState state = event.state();
                if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }

                if (state.nodes().isLocalNodeElectedMaster()) {
                    if (state.custom("test") == null) {
                        if (installed.compareAndSet(false, true)) {
                            clusterService.submitUnbatchedStateUpdateTask(
                                "install-metadata-custom",
                                new ClusterStateUpdateTask(Priority.URGENT) {

                                    @Override
                                    public ClusterState execute(ClusterState currentState) {
                                        if (currentState.custom("test") == null) {
                                            final ClusterState.Builder builder = ClusterState.builder(currentState);
                                            builder.putCustom("test", new TestCustom(42));
                                            return builder.build();
                                        } else {
                                            return currentState;
                                        }
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        throw new AssertionError(e);
                                    }

                                }
                            );
                        }
                    }
                }

            });
            return Collections.emptyList();
        }
    }

    public void testNodeLeftGeneration() throws IOException {
        final var clusterService = internalCluster().getInstance(ClusterService.class);
        final var initialGeneration = clusterService.state().nodes().getNodeLeftGeneration();
        assertThat(clusterService.state().toString(), containsString("node-left generation: " + initialGeneration));

        final var newNode = internalCluster().startNode();
        assertEquals(initialGeneration, clusterService.state().nodes().getNodeLeftGeneration());
        internalCluster().stopNode(newNode);
        assertEquals(initialGeneration + 1, clusterService.state().nodes().getNodeLeftGeneration());

        assertThat(clusterService.state().toString(), containsString("node-left generation: " + (initialGeneration + 1)));
    }

}
