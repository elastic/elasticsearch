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

public class SecurityMigrationProgressMetadataTests extends AbstractChunkedSerializingTestCase<SecurityMigrationProgressMetadata> {

    @Override
    protected SecurityMigrationProgressMetadata doParseInstance(XContentParser parser) throws IOException {
        return SecurityMigrationProgressMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<SecurityMigrationProgressMetadata> instanceReader() {
        return SecurityMigrationProgressMetadata::new;
    }

    @Override
    protected SecurityMigrationProgressMetadata createTestInstance() {
        if (randomBoolean()) {
            return SecurityMigrationProgressMetadata.EMPTY;
        }
        return new SecurityMigrationProgressMetadata(SecurityMigrationProgressMetadata.Phase.RUNNING, randomNonNegativeLong());
    }

    @Override
    protected SecurityMigrationProgressMetadata mutateInstance(SecurityMigrationProgressMetadata instance) throws IOException {
        if (instance == SecurityMigrationProgressMetadata.EMPTY || instance.phase() == SecurityMigrationProgressMetadata.Phase.IDLE) {
            return new SecurityMigrationProgressMetadata(SecurityMigrationProgressMetadata.Phase.RUNNING, randomNonNegativeLong());
        }
        return new SecurityMigrationProgressMetadata(instance.phase(), instance.generation() + randomLongBetween(1, 127));
    }

    public void testXContentDefaults() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            SecurityMigrationProgressMetadata parsed = SecurityMigrationProgressMetadata.fromXContent(parser);
            assertSame(SecurityMigrationProgressMetadata.Phase.IDLE, parsed.phase());
            assertEquals(0L, parsed.generation());
        }
    }

    public void testXContentParsingExamples() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "idle",
              "generation": 0
            }
            """)) {
            SecurityMigrationProgressMetadata parsed = SecurityMigrationProgressMetadata.fromXContent(parser);
            assertSame(SecurityMigrationProgressMetadata.Phase.IDLE, parsed.phase());
            assertEquals(0L, parsed.generation());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "phase": "running",
              "generation": 42
            }
            """)) {
            SecurityMigrationProgressMetadata parsed = SecurityMigrationProgressMetadata.fromXContent(parser);
            assertSame(SecurityMigrationProgressMetadata.Phase.RUNNING, parsed.phase());
            assertEquals(42L, parsed.generation());
        }
    }

    public void testXContentParsingRejectUnknownPhase() throws IOException {
        String json = """
            {
              "phase": "unknown",
              "generation": 0
            }
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            expectThrows(XContentParseException.class, () -> SecurityMigrationProgressMetadata.fromXContent(parser));
        }
    }

    public void testPhaseParseRejectsUnknownValue() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SecurityMigrationProgressMetadata.Phase.parse("unknown")
        );
        assertThat(e.getMessage(), containsString("Unexpected security migration phase"));
    }

    public void testConstructorRejectsNegativeGeneration() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new SecurityMigrationProgressMetadata(SecurityMigrationProgressMetadata.Phase.RUNNING, -1L)
        );
    }

    public void testConstructorRejectsIdleWithNonZeroGeneration() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new SecurityMigrationProgressMetadata(SecurityMigrationProgressMetadata.Phase.IDLE, 9L)
        );
        assertThat(e.getMessage(), containsString("IDLE phase requires generation zero"));
    }

    public void testChunkCount() {
        AbstractChunkedSerializingTestCase.assertChunkCount(SecurityMigrationProgressMetadata.EMPTY, ignored -> 2);
    }
}
