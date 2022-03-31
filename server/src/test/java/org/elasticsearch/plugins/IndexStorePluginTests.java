/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

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
            IllegalStateException.class,
            () -> new MockNode(settings, Collections.singletonList(ConflictingStorePlugin.class))
        );
        assertThat(
            e,
            hasToString(containsString("registered index store type [" + ConflictingStorePlugin.TYPE + "] conflicts with a built-in type"))
        );
    }

    public void testDuplicateIndexStoreFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new MockNode(settings, Arrays.asList(BarStorePlugin.class, FooStorePlugin.class))
        );
        assertThat(
            e,
            hasToString(
                matches(
                    "java.lang.IllegalStateException: Duplicate key store \\(attempted merging values "
                        + "org.elasticsearch.index.store.FsDirectoryFactory@[\\w\\d]+ "
                        + "and org.elasticsearch.index.store.FsDirectoryFactory@[\\w\\d]+\\)"
                )
            )
        );
    }

    public void testDuplicateIndexStoreRecoveryStateFactories() {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new MockNode(settings, Arrays.asList(FooCustomRecoveryStore.class, BarCustomRecoveryStore.class))
        );
        assertThat(e.getMessage(), containsString("Duplicate key recovery-type"));
    }
}
