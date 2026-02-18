/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

public class StreamsMetadataTests extends AbstractChunkedSerializingTestCase<StreamsMetadata> {
    @Override
    protected StreamsMetadata doParseInstance(XContentParser parser) throws IOException {
        return StreamsMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<StreamsMetadata> instanceReader() {
        return StreamsMetadata::new;
    }

    @Override
    protected StreamsMetadata createTestInstance() {
        return new StreamsMetadata(randomBoolean(), randomBoolean(), randomBoolean());
    }

    @Override
    protected StreamsMetadata mutateInstance(StreamsMetadata instance) throws IOException {
        return switch (between(0, 2)) {
            case 0 -> new StreamsMetadata(instance.logsEnabled == false, instance.logsECSEnabled, instance.logsOTelEnabled);
            case 1 -> new StreamsMetadata(instance.logsEnabled, instance.logsECSEnabled == false, instance.logsOTelEnabled);
            case 2 -> new StreamsMetadata(instance.logsEnabled, instance.logsECSEnabled, instance.logsOTelEnabled == false);
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    public void testParsing() throws IOException {
        String emptyMeta = "{}";
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, emptyMeta)) {
            StreamsMetadata meta = StreamsMetadata.fromXContent(jsonParser);
            assertFalse(meta.isLogsEnabled());
            assertFalse(meta.isLogsOTelEnabled());
            assertFalse(meta.isLogsECSEnabled());
        }
        String onlyLogs = """
            {
              "logs_enabled": true
            }
            """;
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, onlyLogs)) {
            StreamsMetadata meta = StreamsMetadata.fromXContent(jsonParser);
            assertTrue(meta.isLogsEnabled());
            assertFalse(meta.isLogsOTelEnabled());
            assertFalse(meta.isLogsECSEnabled());
        }
        String onlyLogsOtel = """
            {
              "logs_otel_enabled": true
            }
            """;
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, onlyLogsOtel)) {
            StreamsMetadata meta = StreamsMetadata.fromXContent(jsonParser);
            assertFalse(meta.isLogsEnabled());
            assertTrue(meta.isLogsOTelEnabled());
            assertFalse(meta.isLogsECSEnabled());
        }
        String onlyLogsECS = """
            {
              "logs_ecs_enabled": true
            }
            """;
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, onlyLogsECS)) {
            StreamsMetadata meta = StreamsMetadata.fromXContent(jsonParser);
            assertFalse(meta.isLogsEnabled());
            assertFalse(meta.isLogsOTelEnabled());
            assertTrue(meta.isLogsECSEnabled());
        }
        String allEnabled = """
            {
              "logs_enabled": true,
              "logs_otel_enabled": true,
              "logs_ecs_enabled": true
            }
            """;
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, allEnabled)) {
            StreamsMetadata meta = StreamsMetadata.fromXContent(jsonParser);
            assertTrue(meta.isLogsEnabled());
            assertTrue(meta.isLogsOTelEnabled());
            assertTrue(meta.isLogsECSEnabled());
        }
    }
}
