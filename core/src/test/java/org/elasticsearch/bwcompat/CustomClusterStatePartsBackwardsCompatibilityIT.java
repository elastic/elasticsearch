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
package org.elasticsearch.bwcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.CustomPrototypeRegistry;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class CustomClusterStatePartsBackwardsCompatibilityIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestPlugin.class, DummyScriptPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder setting = Settings.builder();
        setting.put(super.nodeSettings(nodeOrdinal));
        setting.put("repositories.url.allowed_urls", "http://snapshot.test*");
        return setting.build();
    }

    public void testUpgradeFrom5dot0() throws Exception {
        setupNode();
        ClusterState state = client().admin().cluster().prepareState().all().get().getState();
        ScriptMetaData scriptMetaData = state.getMetaData().custom(ScriptMetaData.TYPE);
        assertThat(scriptMetaData.getScript(MockScriptPlugin.NAME, "key"), equalTo("key"));

        RepositoriesMetaData repositoriesMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);
        RepositoryMetaData repo = repositoriesMetaData.repository("repo");
        assertThat(repo.type(), equalTo("url"));
        assertThat(repo.settings().get("url"), equalTo("http://snapshot.test"));

        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        PipelineConfiguration pipelineConfiguration = ingestMetadata.getPipelines().get("id");
        assertThat(pipelineConfiguration.getId(), equalTo("id"));
        assertThat(pipelineConfiguration.getConfigAsMap(), equalTo(Collections.singletonMap("processors", new ArrayList<>())));

        assertThat(state.metaData().indices().size(), equalTo(1));
        assertThat(state.metaData().indices().get("test"), notNullValue());
        assertThat(state.metaData().indices().get("test").getCreationVersion(), equalTo(Version.V_5_0_0));
        assertThat(state.metaData().indices().get("test").getUpgradedVersion(), equalTo(Version.CURRENT));
        assertThat(state.metaData().indices().get("test").getCustoms().size(), equalTo(1));
        TestPlugin.IMDC imdc = state.metaData().indices().get("test").custom("custom");
        assertThat(imdc.field, equalTo("value"));
    }

    private void setupNode() throws Exception {
        Path unzipDir = createTempDir();
        try (InputStream stream = CustomClusterStatePartsBackwardsCompatibilityIT.class.
            getResourceAsStream("/indices/bwc/custom_cluster_state_parts-5.0.0.zip")) {
            TestUtil.unzip(stream, unzipDir);
        }

        Settings.Builder nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), unzipDir.resolve("data"));
        internalCluster().startNode(nodeSettings.build());
        ensureYellow();
    }

    public static class TestPlugin extends Plugin implements ClusterPlugin, ScriptPlugin {

        @Override
        public Collection<IndexMetaData.Custom> getCustomIndexMetadata() {
            return Collections.singleton(IMDC.PROTO);
        }

        public static class IMDC implements IndexMetaData.Custom {

            public static final IMDC PROTO = new IMDC(null);

            private final String field;

            public IMDC(String field) {
                this.field = field;
            }

            @Override
            public Diff<IndexMetaData.Custom> diff(IndexMetaData.Custom previousState) {
                return new IMDCDiff((IMDC) previousState);
            }

            @Override
            public Diff<IndexMetaData.Custom> readDiffFrom(StreamInput in, CustomPrototypeRegistry registry) throws IOException {
                IndexMetaData.Custom imdc = in.readNamedWriteable(IndexMetaData.Custom.class);
                return new IMDCDiff((IMDC) imdc);
            }

            @Override
            public IndexMetaData.Custom readFrom(StreamInput in) throws IOException {
                return new IMDC(in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(field);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("field", field);
                return builder;
            }

            @Override
            public String type() {
                return "custom";
            }

            @Override
            public IndexMetaData.Custom fromMap(Map<String, Object> map) throws IOException {
                return new IMDC((String) map.get("field"));
            }

            @Override
            public IndexMetaData.Custom fromXContent(XContentParser parser) throws IOException {
                String fieldValue = null;
                for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
                    if (token.isValue()) {
                        fieldValue = parser.text();
                    }
                }
                return new IMDC(fieldValue);
            }

            @Override
            public IndexMetaData.Custom mergeWith(IndexMetaData.Custom another) {
                IMDC other = (IMDC) another;
                return new IMDC(field + other.field);
            }

            static class IMDCDiff implements Diff<IndexMetaData.Custom> {

                private final IMDC previous;

                IMDCDiff(IMDC previous) {
                    this.previous = previous;
                }

                @Override
                public IndexMetaData.Custom apply(IndexMetaData.Custom part) {
                    return new IMDC(((IMDC) part).field);
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeNamedWriteable(previous);
                }
            }
        }
    }

    public static class DummyScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("key", params -> "value");
        }
    }

}
