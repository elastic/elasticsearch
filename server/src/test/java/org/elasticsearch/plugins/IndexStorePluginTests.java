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

package org.elasticsearch.plugins;

import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.transport.MockTcpTransportPlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class IndexStorePluginTests extends ESTestCase {

    public static class BarStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories() {
            return Collections.singletonMap("store", IndexStore::new);
        }

    }

    public static class FooStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories() {
            return Collections.singletonMap("store", IndexStore::new);
        }

    }

    public static class ConflictingStorePlugin extends Plugin implements IndexStorePlugin {

        public static final String TYPE;

        static {
            TYPE = randomFrom(Arrays.asList(IndexModule.Type.values())).getSettingsKey();
        }

        @Override
        public Map<String, Function<IndexSettings, IndexStore>> getIndexStoreFactories() {
            return Collections.singletonMap(TYPE, IndexStore::new);
        }

    }

    public void testIndexStoreFactoryConflictsWithBuiltInIndexStoreType() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
                IllegalStateException.class, () -> new MockNode(settings, Collections.singletonList(ConflictingStorePlugin.class)));
        assertThat(e, hasToString(containsString(
                "registered index store type [" + ConflictingStorePlugin.TYPE + "] conflicts with a built-in type")));
    }

    public void testAllowedIndexStoreTypes() throws IOException {
        final List<String> types =
                Arrays.stream(IndexModule.Type.values()).map(IndexModule.Type::getSettingsKey).collect(Collectors.toList());
        final List<Class<? extends Plugin>> plugins =
                Stream.concat(Stream.of(BarStorePlugin.class), getNecessaryPlugins().stream()).collect(Collectors.toList());
        {
            final Settings settings =
                    Settings.builder()
                            .put("node.allowed_index_store_types", "")
                            .put("path.home", createTempDir())
                            .put("transport.type", MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME)
                            .build();
            // nothing bad should happen when all types are allowed
            try (Node ignored = new MockNode(settings, plugins)) {

            }
        }

        {
            final String allowedBuiltInTypes = String.join(",", randomSubsetOf(types));
            final String allowedIndexStoreTypes =
                    randomBoolean()
                            ? allowedBuiltInTypes
                            : allowedBuiltInTypes.isEmpty() ? "store" : String.join(",", allowedBuiltInTypes, "store");
            final Settings settings =
                    Settings.builder()
                            .put("node.allowed_index_store_types", allowedIndexStoreTypes)
                            .put("path.home", createTempDir())
                            .put("transport.type", MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME)
                            .build();
            // nothing bad should happen when all the types exist
            try (Node ignored = new MockNode(settings, plugins)) {

            }
        }

        {
            final String allowedBuiltInTypes = String.join(",", randomSubsetOf(types));
            final String allowedIndexStoreTypes =
                    randomBoolean()
                            ? allowedBuiltInTypes
                            : allowedBuiltInTypes.isEmpty() ? "store" : String.join(",", allowedBuiltInTypes, "store");
            final String allowedIndexStoreTypesAndNonExisting =
                    allowedIndexStoreTypes.isEmpty() ? "non-existent" : String.join(",", allowedIndexStoreTypes, "non-existent");
            final Settings settings =
                    Settings.builder()
                            .put("node.allowed_index_store_types", allowedIndexStoreTypesAndNonExisting)
                            .put("path.home", createTempDir())
                            .put("transport.type", MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME)
                            .build();
            // we should not be able to specify as allowed a store type that does not exist
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MockNode(settings, plugins));
            assertThat(e, hasToString(containsString("allowed index store type [non-existent] does not exist")));
        }
    }

    private List<Class<? extends Plugin>> getNecessaryPlugins() {
        return Arrays.asList(MockHttpTransport.TestPlugin.class, MockTcpTransportPlugin.class);
    }

    public void testDuplicateIndexStoreFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
                IllegalStateException.class, () -> new MockNode(settings, Arrays.asList(BarStorePlugin.class, FooStorePlugin.class)));
        if (JavaVersion.current().compareTo(JavaVersion.parse("9")) >= 0) {
            assertThat(e, hasToString(matches(
                    "java.lang.IllegalStateException: Duplicate key store \\(attempted merging values " +
                            "org.elasticsearch.plugins.IndexStorePluginTests\\$BarStorePlugin.* " +
                            "and org.elasticsearch.plugins.IndexStorePluginTests\\$FooStorePlugin.*\\)")));
        } else {
            assertThat(e, hasToString(matches(
                    "java.lang.IllegalStateException: Duplicate key org.elasticsearch.plugins.IndexStorePluginTests\\$BarStorePlugin.*")));
        }
    }

}
