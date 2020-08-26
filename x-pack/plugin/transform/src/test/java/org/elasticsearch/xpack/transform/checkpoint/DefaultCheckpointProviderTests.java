/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.MockLogAppender.LoggingExpectation;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor.AuditExpectation;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.mock;

public class DefaultCheckpointProviderTests extends ESTestCase {

    private Client client;

    private MockTransformAuditor transformAuditor;
    private IndexBasedTransformConfigManager transformConfigManager;
    private Logger checkpointProviderlogger = LogManager.getLogger(DefaultCheckpointProvider.class);

    @Before
    public void setUpMocks() throws IllegalAccessException {
        client = mock(Client.class);
        transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        transformAuditor = new MockTransformAuditor();
    }

    public void testReportSourceIndexChangesRunsEmpty() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "warn when source is empty",
                checkpointProviderlogger.getName(),
                Level.WARN,
                "[" + transformId + "] Source did not resolve to any open indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "warn when source is empty",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any open indexes"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.emptySet()); }
        );

        assertExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "do not warn if empty again",
                checkpointProviderlogger.getName(),
                Level.WARN,
                "Source did not resolve to any concrete indexes"
            ),
            new MockTransformAuditor.UnseenAuditExpectation(
                "do not warn if empty again",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any concrete indexes"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.emptySet(), Collections.emptySet()); }
        );
    }

    public void testReportSourceIndexChangesAddDelete() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.singleton("other_index")); }
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            () -> { provider.reportSourceIndexChanges(Sets.newHashSet("index", "other_index"), Collections.singleton("other_index")); }
        );
        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Sets.newHashSet("index", "other_index")); }
        );
    }

    public void testReportSourceIndexChangesAddDeleteMany() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        HashSet<String> oldSet = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            oldSet.add(String.valueOf(i));
        }
        HashSet<String> newSet = new HashSet<>();
        for (int i = 50; i < 150; ++i) {
            newSet.add(String.valueOf(i));
        }

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            () -> { provider.reportSourceIndexChanges(oldSet, newSet); }
        );
    }

    private void assertExpectation(LoggingExpectation loggingExpectation, AuditExpectation auditExpectation, Runnable codeBlock)
        throws IllegalAccessException {
        MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.start();

        Loggers.setLevel(checkpointProviderlogger, Level.DEBUG);
        mockLogAppender.addExpectation(loggingExpectation);

        // always start fresh
        transformAuditor.reset();
        transformAuditor.addExpectation(auditExpectation);
        try {
            Loggers.addAppender(checkpointProviderlogger, mockLogAppender);
            codeBlock.run();
            mockLogAppender.assertAllExpectationsMatched();
            transformAuditor.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(checkpointProviderlogger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

}
