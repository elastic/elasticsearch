/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.XContentTestUtils.convertToXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SystemIndexManagerIT extends ESIntegTestCase {

    private static final String INDEX_NAME = ".test-index";
    private static final String PRIMARY_INDEX_NAME = INDEX_NAME + "-1";

    @Before
    public void beforeEach() {
        TestSystemIndexDescriptor.useNewMappings.set(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    /**
     * Check that if the the SystemIndexManager finds a managed index with out-of-date mappings, then
     * the manager updates those mappings.
     */
    public void testSystemIndexManagerUpgradesMappings() throws Exception {
        internalCluster().startNodes(1);

        // Trigger the creation of the system index
        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        assertMappings(TestSystemIndexDescriptor.getOldMappings());

        // Poke the test descriptor so that the mappings are now "updated"
        TestSystemIndexDescriptor.useNewMappings.set(true);

        // Cause a cluster state update, so that the SystemIndexManager will update the mappings in our index
        triggerClusterStateUpdates();

        assertBusy(() -> assertMappings(TestSystemIndexDescriptor.getNewMappings()));
    }

    /**
     * Check that if the the SystemIndexManager finds a managed index with mappings that claim to be newer than
     * what it expects, then those mappings are left alone.
     */
    public void testSystemIndexManagerLeavesNewerMappingsAlone() throws Exception {
        TestSystemIndexDescriptor.useNewMappings.set(true);

        internalCluster().startNodes(1);
        // Trigger the creation of the system index
        assertAcked(prepareCreate(INDEX_NAME));
        ensureGreen(INDEX_NAME);

        assertMappings(TestSystemIndexDescriptor.getNewMappings());

        // Poke the test descriptor so that the mappings are now out-dated.
        TestSystemIndexDescriptor.useNewMappings.set(false);

        // Cause a cluster state update, so that the SystemIndexManager's listener will execute
        triggerClusterStateUpdates();

        // Mappings should be unchanged.
        assertBusy(() -> assertMappings(TestSystemIndexDescriptor.getNewMappings()));
    }

    /**
     * Performs a cluster state update in order to trigger any cluster state listeners - specifically, SystemIndexManager.
     */
    private void triggerClusterStateUpdates() {
        final String name = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        client().admin().indices().putTemplate(new PutIndexTemplateRequest(name).patterns(List.of(name))).actionGet();
    }

    /**
     * Fetch the mappings for {@link #INDEX_NAME} and verify that they match the expected mappings. Note that this is just
     * a dumb string comparison, so order of keys matters.
     */
    private void assertMappings(String expectedMappings) {
        client().admin().indices().getMappings(new GetMappingsRequest().indices(INDEX_NAME), new ActionListener<>() {
            @Override
            public void onResponse(GetMappingsResponse getMappingsResponse) {
                final ImmutableOpenMap<String, MappingMetadata> mappings = getMappingsResponse.getMappings();
                assertThat(
                    "Expected mappings to contain a key for [" + PRIMARY_INDEX_NAME + "], but found: " + mappings.toString(),
                    mappings.containsKey(PRIMARY_INDEX_NAME),
                    equalTo(true)
                );
                final Map<String, Object> sourceAsMap = mappings.get(PRIMARY_INDEX_NAME).getSourceAsMap();

                try {
                    assertThat(convertToXContent(sourceAsMap, XContentType.JSON).utf8ToString(), equalTo(expectedMappings));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Couldn't fetch mappings for " + INDEX_NAME, e);
            }
        });
    }

    /** A special kind of {@link SystemIndexDescriptor} that can toggle what kind of mappings it
     * expects. A real descriptor is immutable. */
    public static class TestSystemIndexDescriptor extends SystemIndexDescriptor {

        public static final AtomicBoolean useNewMappings = new AtomicBoolean(false);
        private static final Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();

        TestSystemIndexDescriptor() {
            super(INDEX_NAME + "*", PRIMARY_INDEX_NAME, "Test system index", null, settings, INDEX_NAME, 0, "version", "stack");
        }

        @Override
        public boolean isAutomaticallyManaged() {
            return true;
        }

        @Override
        public String getMappings() {
            return useNewMappings.get() ? getNewMappings() : getOldMappings();
        }

        public static String getOldMappings() {
            try {
                final XContentBuilder builder = jsonBuilder();

                builder.startObject();
                {
                    builder.startObject("_meta");
                    builder.field("version", Version.CURRENT.previousMajor().toString());
                    builder.endObject();

                    builder.startObject("properties");
                    {
                        builder.startObject("foo");
                        builder.field("type", "text");
                        builder.endObject();
                    }
                    builder.endObject();
                }

                builder.endObject();
                return Strings.toString(builder);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to build .test-index-1 index mappings", e);
            }
        }

        public static String getNewMappings() {
            try {
                final XContentBuilder builder = jsonBuilder();

                builder.startObject();
                {
                    builder.startObject("_meta");
                    builder.field("version", Version.CURRENT.toString());
                    builder.endObject();

                    builder.startObject("properties");
                    {
                        builder.startObject("bar");
                        builder.field("type", "text");
                        builder.endObject();
                        builder.startObject("foo");
                        builder.field("type", "text");
                        builder.endObject();
                    }
                    builder.endObject();
                }

                builder.endObject();
                return Strings.toString(builder);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to build .test-index-1 index mappings", e);
            }
        }
    }

    /** Just a test plugin to allow the test descriptor to be installed in the cluster. */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new TestSystemIndexDescriptor());
        }
    }
}
