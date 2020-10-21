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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class IndexStorePluginTests extends ESTestCase {

    public static class BarStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store", new FsDirectoryFactory());
        }

    }

    public static class FooStorePlugin extends Plugin implements IndexStorePlugin {

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store", new FsDirectoryFactory());
        }

    }

    public static class ConflictingStorePlugin extends Plugin implements IndexStorePlugin {

        public static final String TYPE;

        static {
            TYPE = randomFrom(Arrays.asList(IndexModule.Type.values())).getSettingsKey();
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap(TYPE, new FsDirectoryFactory());
        }

    }

    public static class FooCustomRecoveryStore extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store-a", new FsDirectoryFactory());
        }

        @Override
        public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
            return Collections.singletonMap("recovery-type", new RecoveryFactory());
        }
    }

    public static class BarCustomRecoveryStore extends Plugin implements IndexStorePlugin {
        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.singletonMap("store-b", new FsDirectoryFactory());
        }

        @Override
        public Map<String, RecoveryStateFactory> getRecoveryStateFactories() {
            return Collections.singletonMap("recovery-type", new RecoveryFactory());
        }
    }

    public static class RecoveryFactory implements IndexStorePlugin.RecoveryStateFactory {
        @Override
        public RecoveryState newRecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, DiscoveryNode sourceNode) {
            return new RecoveryState(shardRouting, targetNode, sourceNode);
        }
    }


    public void testIndexStoreFactoryConflictsWithBuiltInIndexStoreType() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
                IllegalStateException.class, () -> new MockNode(settings, Collections.singletonList(ConflictingStorePlugin.class)));
        assertThat(e, hasToString(containsString(
                "registered index store type [" + ConflictingStorePlugin.TYPE + "] conflicts with a built-in type")));
    }

    public void testDuplicateIndexStoreFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
                IllegalStateException.class, () -> new MockNode(settings, Arrays.asList(BarStorePlugin.class, FooStorePlugin.class)));
        if (JavaVersion.current().compareTo(JavaVersion.parse("9")) >= 0) {
            assertThat(e, hasToString(matches(
                    "java.lang.IllegalStateException: Duplicate key store \\(attempted merging values " +
                            "org.elasticsearch.index.store.FsDirectoryFactory@[\\w\\d]+ " +
                            "and org.elasticsearch.index.store.FsDirectoryFactory@[\\w\\d]+\\)")));
        } else {
            assertThat(e, hasToString(matches(
                    "java.lang.IllegalStateException: Duplicate key org.elasticsearch.index.store.FsDirectoryFactory@[\\w\\d]+")));
        }
    }

    public void testDuplicateIndexStoreRecoveryStateFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class, () -> new MockNode(settings, Arrays.asList(FooCustomRecoveryStore.class,
                                                                                    BarCustomRecoveryStore.class)));
        if (JavaVersion.current().compareTo(JavaVersion.parse("9")) >= 0) {
            assertThat(e.getMessage(), containsString("Duplicate key recovery-type"));
        } else {
            assertThat(e, hasToString(matches(
                "java.lang.IllegalStateException: Duplicate key " +
                    "org.elasticsearch.plugins.IndexStorePluginTests$RecoveryFactory@[\\w\\d]+")));
        }
    }
}
