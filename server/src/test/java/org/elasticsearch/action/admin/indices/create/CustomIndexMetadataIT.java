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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CustomIndexMetadataIT extends ESIntegTestCase {


    public static final class TestIndexMetaData extends AbstractNamedDiffable<IndexMetaData.Custom> implements IndexMetaData.Custom {
        public static final String TYPE = "testindexmeta";

        private static final ConstructingObjectParser<TestIndexMetaData, Void> PARSER = new ConstructingObjectParser<>(
            "painless_execute_request", args -> new TestIndexMetaData((String) args[0]));

        static {
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("payload"));
        }


        private final String payload;

        public TestIndexMetaData(String payload) {
            this.payload = payload;
        }

        public TestIndexMetaData(StreamInput in) throws IOException {
            this.payload = in.readString();
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_7_0_0_alpha1;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(payload);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("payload", payload);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestIndexMetaData that = (TestIndexMetaData) o;
            return Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(payload);
        }

        public static TestIndexMetaData fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }


        public static NamedDiff<IndexMetaData.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(IndexMetaData.Custom.class, TYPE, in);
        }

        public String getPayload() {
            return payload;
        }

    }

    public static final class CustomMetaDataAdder implements ClusterStateListener {

        private final ClusterService clusterService;
        private final AtomicBoolean updateRunning = new AtomicBoolean();

        public CustomMetaDataAdder(ClusterService clusterService) {
            this.clusterService = clusterService;
            clusterService.addListener(this);
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.localNodeMaster() && event.state().metaData().hasIndex("foobar")) {
                TestIndexMetaData testIndexMetaData = event.state().metaData().index("foobar").custom(TestIndexMetaData.TYPE);
                if (testIndexMetaData == null && updateRunning.getAndSet(true) == false) {
                    clusterService.submitStateUpdateTask("add-index-custom-metadata-test", new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            if (currentState.metaData().hasIndex("foobar")) {
                                IndexMetaData indexMetaData = currentState.metaData().index("foobar");
                                if (indexMetaData.custom(TestIndexMetaData.TYPE) == null) {
                                    ClusterState.Builder builder = ClusterState.builder(currentState);
                                    IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData);
                                    indexMetaDataBuilder.putCustom(TestIndexMetaData.TYPE, new TestIndexMetaData("payload"));
                                    return builder.metaData(MetaData.builder(currentState.metaData()).put(indexMetaDataBuilder)).build();
                                }
                            }

                            return currentState;
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            updateRunning.set(false);
                        }

                        @Override
                        public void onNoLongerMaster(String source) {
                            updateRunning.set(false);
                        }

                        @Override
                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                            updateRunning.set(false);
                        }
                    });
                }
            }

        }
    }

    public static class TestCustomIndexMetadataPlugin extends Plugin {
        @Override
        public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                   ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                   NamedXContentRegistry xContentRegistry, Environment environment,
                                                   NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
            return Collections.singletonList(new CustomMetaDataAdder(clusterService));
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Arrays.asList(
                new NamedWriteableRegistry.Entry(IndexMetaData.Custom.class, TestIndexMetaData.TYPE, TestIndexMetaData::new),
                new NamedWriteableRegistry.Entry(NamedDiff.class, TestIndexMetaData.TYPE, TestIndexMetaData::readDiffFrom)
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return Collections.singletonList(new NamedXContentRegistry.Entry(
                IndexMetaData.Custom.class, new ParseField(TestIndexMetaData.TYPE), TestIndexMetaData::fromXContent));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestCustomIndexMetadataPlugin.class);
        return plugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestCustomIndexMetadataPlugin.class);
        return plugins;

    }

    public void testCreateIndexWithCustomMetaData() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("foobar"));
        assertBusy(() -> {
            ClusterStateResponse response = client().admin().cluster().prepareState().setIndices("foobar").get();
            IndexMetaData indexMetaData = response.getState().getMetaData().index("foobar");
            assertNotNull(indexMetaData);
            TestIndexMetaData testIndexMetaData = indexMetaData.custom(TestIndexMetaData.TYPE);
            assertNotNull(testIndexMetaData);
            assertEquals("payload", testIndexMetaData.getPayload());
        });
    }
}
