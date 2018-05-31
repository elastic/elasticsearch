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
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptMetaData;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class ClusterStateIT extends ESIntegTestCase {

    public static class CustomPlugin extends Plugin {

        public CustomPlugin() {

        }

        static class CustomPluginCustom implements MetaData.Custom {

            @Override
            public Optional<String> getRequiredFeature() {
                return Optional.of("custom");
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
            public String getWriteableName() {
                return TYPE;
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {

            }

            @Override
            public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
                builder.startObject();
                {

                }
                builder.endObject();
                return builder;
            }
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return super.getNamedWriteables();
        }

        public static final String TYPE = "custom_plugin";

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
                    if (metaData.custom(CustomPlugin.TYPE) == null) {
                        if (installed.compareAndSet(false, true)) {
                            clusterService.submitStateUpdateTask("install-metadata-custom", new ClusterStateUpdateTask(Priority.URGENT) {

                                @Override
                                public ClusterState execute(ClusterState currentState) {
                                    if (currentState.custom(CustomPlugin.TYPE) == null) {
                                        final MetaData.Builder builder = MetaData.builder(currentState.metaData());
                                        builder.putCustom(CustomPlugin.TYPE, new CustomPluginCustom());
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(CustomPlugin.class)).collect(Collectors.toCollection(ArrayList::new));
    }

    public void testOptionalCustoms() throws Exception {
        assertBusy(() -> assertTrue(clusterService().state().metaData().customs().containsKey(CustomPlugin.TYPE)));
        final ClusterStateResponse state = internalCluster().transportClient().admin().cluster().prepareState().get();
        final ImmutableOpenMap<String, MetaData.Custom> customs = state.getState().metaData().customs();
        final Set<String> keys = new HashSet<>(Arrays.asList(customs.keys().toArray(String.class)));
        assertThat(keys, hasItem(IndexGraveyard.TYPE));
        assertThat(keys, hasItem(IngestMetadata.TYPE));
        assertThat(keys, hasItem(RepositoriesMetaData.TYPE));
        assertThat(keys, hasItem(ScriptMetaData.TYPE));
        assertThat(keys, not(hasItem(CustomPlugin.TYPE)));
    }

}
