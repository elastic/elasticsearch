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
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class SystemIndexMigrationProgressMetadataTests extends AbstractChunkedSerializingTestCase<SystemIndexMigrationProgressMetadata> {

    @Override
    protected SystemIndexMigrationProgressMetadata doParseInstance(XContentParser parser) throws IOException {
        return SystemIndexMigrationProgressMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SystemIndexMigrationProgressMetadata> instanceReader() {
        return SystemIndexMigrationProgressMetadata::new;
    }

    @Override
    protected SystemIndexMigrationProgressMetadata createTestInstance() {
        if (randomBoolean()) {
            return SystemIndexMigrationProgressMetadata.EMPTY;
        }
        return new SystemIndexMigrationProgressMetadata(
            SystemIndexMigrationProgressMetadata.Phase.RUNNING,
            randomNonNegativeLong(),
            randomAlphaOfLengthBetween(5, 20)
        );
    }

    @Override
    protected SystemIndexMigrationProgressMetadata mutateInstance(SystemIndexMigrationProgressMetadata instance) throws IOException {
        if (instance == SystemIndexMigrationProgressMetadata.EMPTY) {
            return new SystemIndexMigrationProgressMetadata(
                SystemIndexMigrationProgressMetadata.Phase.RUNNING,
                randomNonNegativeLong(),
                randomAlphaOfLengthBetween(3, 9)
            );
        }
        if (randomBoolean()) {
            return new SystemIndexMigrationProgressMetadata(
                instance.phase(),
                instance.generation() + randomLongBetween(1, 64),
                instance.currentFeature()
            );
        }
        return new SystemIndexMigrationProgressMetadata(
            instance.phase(),
            instance.generation(),
            instance.currentFeature() + randomAlphaOfLength(1)
        );
    }

    public void testXContentDefaults() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            SystemIndexMigrationProgressMetadata parsed = SystemIndexMigrationProgressMetadata.fromXContent(parser);
            assertSame(SystemIndexMigrationProgressMetadata.Phase.IDLE, parsed.phase());
            assertEquals(0L, parsed.generation());
            assertNull(parsed.currentFeature());
        }
    }

    public void testXContentParsingExamples() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "running",
              "generation": 42
            }
            """)) {
            SystemIndexMigrationProgressMetadata parsed = SystemIndexMigrationProgressMetadata.fromXContent(parser);
            assertSame(SystemIndexMigrationProgressMetadata.Phase.RUNNING, parsed.phase());
            assertEquals(42L, parsed.generation());
            assertNull(parsed.currentFeature());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "running",
              "generation": 5,
              "current_feature": "security"
            }
            """)) {
            SystemIndexMigrationProgressMetadata parsed = SystemIndexMigrationProgressMetadata.fromXContent(parser);
            assertSame(SystemIndexMigrationProgressMetadata.Phase.RUNNING, parsed.phase());
            assertEquals(5L, parsed.generation());
            assertEquals("security", parsed.currentFeature());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "running",
              "generation": 14,
              "current_feature": null
            }
            """)) {
            SystemIndexMigrationProgressMetadata parsed = SystemIndexMigrationProgressMetadata.fromXContent(parser);
            assertSame(SystemIndexMigrationProgressMetadata.Phase.RUNNING, parsed.phase());
            assertEquals(14L, parsed.generation());
            assertNull(parsed.currentFeature());
        }
    }

    public void testXContentParsingRejectUnknownPhase() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "paused",
              "generation": 0
            }
            """)) {
            expectThrows(XContentParseException.class, () -> SystemIndexMigrationProgressMetadata.fromXContent(parser));
        }
    }

    public void testPhaseParseRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SystemIndexMigrationProgressMetadata.Phase.parse("boom")
        );
        assertThat(e.getMessage(), containsString("Unexpected system index migration phase"));
    }

    public void testConstructorRejectsNegativeGeneration() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new SystemIndexMigrationProgressMetadata(SystemIndexMigrationProgressMetadata.Phase.RUNNING, -1L, null)
        );
    }

    public void testConstructorRejectsIdleWithNonZeroGeneration() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SystemIndexMigrationProgressMetadata(SystemIndexMigrationProgressMetadata.Phase.IDLE, 1L, null)
        );
        assertThat(e.getMessage(), containsString("IDLE phase requires generation zero"));
    }

    public void testConstructorRejectsIdleWhileTrackingFeature() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SystemIndexMigrationProgressMetadata(SystemIndexMigrationProgressMetadata.Phase.IDLE, 0L, "watcher")
        );
        assertThat(e.getMessage(), containsString("IDLE phase cannot track a feature"));
    }

    public void testChunkCounts() throws IOException {
        AbstractChunkedSerializingTestCase.assertChunkCount(SystemIndexMigrationProgressMetadata.EMPTY, ignored -> 2);
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new SystemIndexMigrationProgressMetadata(SystemIndexMigrationProgressMetadata.Phase.RUNNING, 1L, null),
            ignored -> 2
        );
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new SystemIndexMigrationProgressMetadata(SystemIndexMigrationProgressMetadata.Phase.RUNNING, 2L, "security"),
            ignored -> 3
        );
    }
}
