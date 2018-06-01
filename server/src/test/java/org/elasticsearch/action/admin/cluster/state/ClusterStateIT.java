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

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;

@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class ClusterStateIT extends ESIntegTestCase {

    public abstract static class CustomPluginCustom implements MetaData.Custom {

        private static final ParseField VALUE = new ParseField("value");

        final int value;

        public CustomPluginCustom(int value) {
            this.value = value;
        }

        public CustomPluginCustom(final StreamInput in) throws IOException {
            value = in.readInt();
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return MetaData.ALL_CONTEXTS;
        }

        @Override
        public Diff<MetaData.Custom> diff(final MetaData.Custom previousState) {
            return null;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeInt(value);
        }

        public static int fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token;
            int value = 0;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case VALUE_BOOLEAN:
                        if (VALUE.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.intValue();
                        }
                        break;
                }
            }
            return value;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(VALUE.getPreferredName(), value);
            return builder;
        }
    }

    public static class CustomPluginCustom1 extends CustomPluginCustom {

        public static final String TYPE = "custom_plugin_1";

        public CustomPluginCustom1(int value) {
            super(value);
        }

        public CustomPluginCustom1(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

    }

    public static class CustomPluginCustom2 extends CustomPluginCustom {

        public static final String TYPE = "custom_plugin_2";

        public CustomPluginCustom2(int value) {
            super(value);
        }

        public CustomPluginCustom2(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

    }

    public abstract static class CustomPlugin extends Plugin {

        private final List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>();
        private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

        public CustomPlugin() {
            registerBuiltinWritables();
        }

        protected <T extends MetaData.Custom> void registerMetaDataCustom(String name, Writeable.Reader<T> reader,
                                                                          CheckedFunction<XContentParser, T, IOException> parser) {
            namedWritables.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, name, reader));
            namedXContents.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(name), parser));
        }

        protected abstract void registerBuiltinWritables();

        protected abstract String getType();

        protected abstract CustomPluginCustom getInstance();

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return namedWritables;
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return namedXContents;
        }


        private final AtomicBoolean installed = new AtomicBoolean();

        @Override
        public Collection<Object> createComponents(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService,
            final NamedXContentRegistry xContentRegistry,
            final Environment environment,
            final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry) {
            clusterService.addListener(event -> {
                final ClusterState state = event.state();
                if (state.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }

                final MetaData metaData = state.metaData();
                if (state.nodes().isLocalNodeElectedMaster()) {
                    if (metaData.custom(getType()) == null) {
                        if (installed.compareAndSet(false, true)) {
                            clusterService.submitStateUpdateTask("install-metadata-custom", new ClusterStateUpdateTask(Priority.URGENT) {

                                @Override
                                public ClusterState execute(ClusterState currentState) {
                                    if (currentState.custom(getType()) == null) {
                                        final MetaData.Builder builder = MetaData.builder(currentState.metaData());
                                        builder.putCustom(getType(), getInstance());
                                        return ClusterState.builder(currentState).metaData(builder).build();
                                    } else {
                                        return currentState;
                                    }
                                }

                                @Override
                                public void onFailure(String source, Exception e) {
                                    throw new AssertionError(e);
                                }

                            });
                        }
                    }
                }

            });
            return Collections.emptyList();
        }
    }

    public static class CustomPlugin1 extends CustomPlugin {

        @Override
        protected void registerBuiltinWritables() {
            registerMetaDataCustom(CustomPluginCustom1.TYPE, CustomPluginCustom1::new,
                parser -> new CustomPluginCustom1(CustomPluginCustom1.fromXContent(parser)));
        }

        @Override
        protected String getType() {
            return CustomPluginCustom1.TYPE;
        }

        @Override
        protected CustomPluginCustom getInstance() {
            return new CustomPluginCustom1(42);
        }
    }

    public static class CustomPlugin2 extends CustomPlugin {

        @Override
        protected void registerBuiltinWritables() {
            registerMetaDataCustom(CustomPluginCustom2.TYPE, CustomPluginCustom2::new,
                parser -> new CustomPluginCustom2(CustomPluginCustom2.fromXContent(parser)));
        }

        @Override
        protected String getType() {
            return CustomPluginCustom2.TYPE;
        }

        @Override
        protected CustomPluginCustom getInstance() {
            return new CustomPluginCustom1(13);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomPlugin1.class, CustomPlugin2.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(CustomPlugin1.class); // only CustomPlugin1
    }

    public void testOptionalCustoms() throws Exception {
        assertBusy(() -> assertTrue(clusterService().state().metaData().customs().containsKey(CustomPluginCustom1.TYPE)));
        assertBusy(() -> assertTrue(clusterService().state().metaData().customs().containsKey(CustomPluginCustom2.TYPE)));
        final ClusterStateResponse state = internalCluster().transportClient().admin().cluster().prepareState().get();
        final ImmutableOpenMap<String, MetaData.Custom> customs = state.getState().metaData().customs();
        final Set<String> keys = new HashSet<>(Arrays.asList(customs.keys().toArray(String.class)));
        assertThat(keys, hasItem(IndexGraveyard.TYPE));
        assertThat(keys, hasItem(CustomPluginCustom1.TYPE));
        assertThat(keys, not(hasItem(CustomPluginCustom2.TYPE)));
    }

}
