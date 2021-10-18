/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.TimeUnit;

public class NumberOfReplicasUpdateTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static ClusterStateChanges clusterStateChanges;
    private static ClusterState clusterState;
    private static Logger deprecationLogger;

    @BeforeClass
    public static void setup() {
        threadPool = new TestThreadPool("test");
        clusterStateChanges = new ClusterStateChanges(NamedXContentRegistry.EMPTY, threadPool);

        final Settings.Builder regularSettings = Settings.builder();
        if (randomBoolean()) {
            regularSettings.put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false");
        }

        final Settings.Builder autoExpandSettings = Settings.builder();
        autoExpandSettings.put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all");

        ClusterState clusterState = ClusterState.EMPTY_STATE;
        clusterState = clusterStateChanges.createIndex(clusterState, new CreateIndexRequest("regular", regularSettings.build()));
        clusterState = clusterStateChanges.createIndex(clusterState, new CreateIndexRequest("auto-expands", autoExpandSettings.build()));
        NumberOfReplicasUpdateTests.clusterState = clusterState;

        deprecationLogger = LogManager.getLogger("org.elasticsearch.deprecation.cluster.metadata.MetadataUpdateSettingsService");
    }

    @AfterClass
    public static void terminateThreadpool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        threadPool = null;
        clusterStateChanges = null;
        clusterState = null;
        deprecationLogger = null;
    }

    public void testDeprecatedIfAutoExpandReplicasSet() {
        assertDeprecated(numberOfReplicasUpdate(), "auto-expands");
    }

    public void testNotDeprecatedIfAutoExpandReplicasUnset() {
        assertNotDeprecated(numberOfReplicasUpdate(), "regular");
    }

    public void testNotDeprecatedIfAutoExpandReplicasUnsetOnSomeIndices() {
        assertNotDeprecated(numberOfReplicasUpdate(), "regular", "auto-expands");
    }

    public void testNotDeprecatedIfDisablingAutoExpandReplicas() {
        assertNotDeprecated(numberOfReplicasUpdate().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "false"), randomIndices());
    }

    public void testNotDeprecatedIfClearingAutoExpandReplicas() {
        assertNotDeprecated(numberOfReplicasUpdate().putNull(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS), randomIndices());
    }

    public void testDeprecatedIfSettingAutoExpandReplicas() {
        assertDeprecated(numberOfReplicasUpdate().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"), randomIndices());
    }

    public void testNotDeprecatedIfNoIndicesSelected() {
        runTest(
            false,
            new UpdateSettingsRequest(numberOfReplicasUpdate().build(), "nonexistent*")
                .indicesOptions(IndicesOptions.lenientExpandOpen()));
    }

    private static Settings.Builder numberOfReplicasUpdate() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, Integer.toString(between(0, 5)));
    }

    private static String[] randomIndices() {
        return randomSubsetOf(between(1, 2), "regular", "auto-expands").toArray(new String[0]);
    }

    private static void assertDeprecated(Settings.Builder settings, String... indices) {
        runTest(true, settings, indices);
    }

    private static void assertNotDeprecated(Settings.Builder settings, String... indices) {
        runTest(false, settings, indices);
    }

    private static void runTest(boolean expectDeprecated, Settings.Builder settings, String[] indices) {
        runTest(expectDeprecated, new UpdateSettingsRequest(settings.build(), indices));
    }

    private static void runTest(boolean expectDeprecated, UpdateSettingsRequest updateSettingsRequest) {
        try {
            final MockLogAppender appender = new MockLogAppender();
            try {
                appender.start();
                Loggers.addAppender(deprecationLogger, appender);

                if (expectDeprecated) {
                    appender.addExpectation(new MockLogAppender.SeenEventExpectation(
                        "deprecation",
                        deprecationLogger.getName(),
                        DeprecationLogger.DEPRECATION,
                        "setting [index.number_of_replicas] on indices using [index.auto_expand_replicas] has no effect so it is " +
                            "deprecated and will be forbidden in a future version"));
                } else {
                    appender.addExpectation(new MockLogAppender.UnseenEventExpectation(
                        "deprecation",
                        deprecationLogger.getName(),
                        DeprecationLogger.DEPRECATION,
                        "*"));
                }

                clusterStateChanges.updateSettings(clusterState, updateSettingsRequest);

                appender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(deprecationLogger, appender);
                appender.stop();
            }
        } catch (IllegalAccessException e) {
            throw new AssertionError("unexpected", e);
        }
    }

}
