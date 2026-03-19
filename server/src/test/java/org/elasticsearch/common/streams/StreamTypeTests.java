/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.streams;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.StreamsMetadata;
import org.elasticsearch.test.ESTestCase;

public class StreamTypeTests extends ESTestCase {
    public void testStreamMatchAndParent() {
        {
            ProjectMetadata noneEnabled = createMetadata(false, false, false);
            assertNull(StreamType.exactEnabledStreamMatch(noneEnabled, "logs"));
            assertNull(StreamType.exactEnabledStreamMatch(noneEnabled, "logs.otel"));
            assertNull(StreamType.exactEnabledStreamMatch(noneEnabled, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs.foo"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs.otel"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs.otel.foo"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(noneEnabled, "logs.ecs.foo"));
        }

        {
            ProjectMetadata logsOnly = createMetadata(true, false, false);
            assertEquals(StreamType.LOGS, StreamType.exactEnabledStreamMatch(logsOnly, "logs"));
            assertNull(StreamType.exactEnabledStreamMatch(logsOnly, "logs.otel"));
            assertNull(StreamType.exactEnabledStreamMatch(logsOnly, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(logsOnly, "logs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsOnly, "logs.foo"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsOnly, "logs.otel"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsOnly, "logs.otel.foo"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsOnly, "logs.ecs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsOnly, "logs.ecs.foo"));
        }

        {
            ProjectMetadata logsAndOTel = createMetadata(true, false, true);
            assertEquals(StreamType.LOGS, StreamType.exactEnabledStreamMatch(logsAndOTel, "logs"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.exactEnabledStreamMatch(logsAndOTel, "logs.otel"));
            assertNull(StreamType.exactEnabledStreamMatch(logsAndOTel, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(logsAndOTel, "logs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndOTel, "logs.foo"));
            assertNull(StreamType.enabledParentStreamOf(logsAndOTel, "logs.otel"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.enabledParentStreamOf(logsAndOTel, "logs.otel.foo"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndOTel, "logs.ecs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndOTel, "logs.ecs.foo"));
        }

        {
            ProjectMetadata logsAndECS = createMetadata(true, true, false);
            assertEquals(StreamType.LOGS, StreamType.exactEnabledStreamMatch(logsAndECS, "logs"));
            assertNull(StreamType.exactEnabledStreamMatch(logsAndECS, "logs.otel"));
            assertEquals(StreamType.LOGS_ECS, StreamType.exactEnabledStreamMatch(logsAndECS, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(logsAndECS, "logs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndECS, "logs.foo"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndECS, "logs.otel"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(logsAndECS, "logs.otel.foo"));
            assertNull(StreamType.enabledParentStreamOf(logsAndECS, "logs.ecs"));
            assertEquals(StreamType.LOGS_ECS, StreamType.enabledParentStreamOf(logsAndECS, "logs.ecs.foo"));
        }

        {
            ProjectMetadata ecsAndOTel = createMetadata(false, true, true);
            assertNull(StreamType.exactEnabledStreamMatch(ecsAndOTel, "logs"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.exactEnabledStreamMatch(ecsAndOTel, "logs.otel"));
            assertEquals(StreamType.LOGS_ECS, StreamType.exactEnabledStreamMatch(ecsAndOTel, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(ecsAndOTel, "logs"));
            assertNull(StreamType.enabledParentStreamOf(ecsAndOTel, "logs.foo"));
            assertNull(StreamType.enabledParentStreamOf(ecsAndOTel, "logs.otel"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.enabledParentStreamOf(ecsAndOTel, "logs.otel.foo"));
            assertNull(StreamType.enabledParentStreamOf(ecsAndOTel, "logs.ecs"));
            assertEquals(StreamType.LOGS_ECS, StreamType.enabledParentStreamOf(ecsAndOTel, "logs.ecs.foo"));
        }

        {
            ProjectMetadata allEnabled = createMetadata(true, true, true);
            assertEquals(StreamType.LOGS, StreamType.exactEnabledStreamMatch(allEnabled, "logs"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.exactEnabledStreamMatch(allEnabled, "logs.otel"));
            assertEquals(StreamType.LOGS_ECS, StreamType.exactEnabledStreamMatch(allEnabled, "logs.ecs"));
            assertNull(StreamType.enabledParentStreamOf(allEnabled, "logs"));
            assertEquals(StreamType.LOGS, StreamType.enabledParentStreamOf(allEnabled, "logs.foo"));
            assertNull(StreamType.enabledParentStreamOf(allEnabled, "logs.otel"));
            assertEquals(StreamType.LOGS_OTEL, StreamType.enabledParentStreamOf(allEnabled, "logs.otel.foo"));
            assertNull(StreamType.enabledParentStreamOf(allEnabled, "logs.ecs"));
            assertEquals(StreamType.LOGS_ECS, StreamType.enabledParentStreamOf(allEnabled, "logs.ecs.foo"));
        }
    }

    private ProjectMetadata createMetadata(boolean logsEnabled, boolean logsECSEnabled, boolean logsOTelEnabled) {
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(StreamsMetadata.TYPE, new StreamsMetadata(logsEnabled, logsECSEnabled, logsOTelEnabled))
            .build();
    }
}
