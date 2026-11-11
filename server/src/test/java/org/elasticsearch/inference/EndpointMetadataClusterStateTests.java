/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.inference.metadata.EndpointMetadataClusterState;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class EndpointMetadataClusterStateTests extends AbstractBWCSerializationTestCase<EndpointMetadataClusterState> {

    public static EndpointMetadataClusterState randomInstance() {
        if (randomBoolean()) {
            return EndpointMetadataClusterState.EMPTY_INSTANCE;
        }
        return new EndpointMetadataClusterState(EndpointMetadataTests.randomHeuristics(), EndpointMetadataTests.randomInternal());
    }

    public static EndpointMetadataClusterState randomNonEmptyInstance() {
        // Guarantee heuristics is non-empty so the result can never equal EMPTY_INSTANCE (which requires both components to be empty).
        var heuristics = randomValueOtherThan(EndpointMetadata.Heuristics.EMPTY_INSTANCE, EndpointMetadataTests::randomHeuristics);
        return new EndpointMetadataClusterState(heuristics, EndpointMetadataTests.randomInternal());
    }

    @Override
    protected EndpointMetadataClusterState createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<EndpointMetadataClusterState> instanceReader() {
        return EndpointMetadataClusterState::new;
    }

    @Override
    protected EndpointMetadataClusterState doParseInstance(XContentParser parser) throws IOException {
        return EndpointMetadataClusterState.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        // The parser is intentionally lenient: unknown fields (e.g. display, regions written by older nodes) must be ignored.
        return true;
    }

    @Override
    protected EndpointMetadataClusterState mutateInstance(EndpointMetadataClusterState instance) throws IOException {
        if (randomBoolean()) {
            return new EndpointMetadataClusterState(
                randomValueOtherThan(instance.heuristics(), EndpointMetadataTests::randomHeuristics),
                instance.internal()
            );
        }
        return new EndpointMetadataClusterState(
            instance.heuristics(),
            randomValueOtherThan(instance.internal(), EndpointMetadataTests::randomInternal)
        );
    }

    @Override
    protected EndpointMetadataClusterState mutateInstanceForVersion(EndpointMetadataClusterState instance, TransportVersion version) {
        return instance;
    }

    public void testToXContent_EmptyInstance() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        EndpointMetadataClusterState.EMPTY_INSTANCE.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {
              "heuristics": {
                "properties": []
              },
              "internal": {}
            }
            """)));
    }

    public void testToXContent_NonEmptyInstance() throws IOException {
        var instance = new EndpointMetadataClusterState(
            new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
            new EndpointMetadata.Internal("fingerprint", 1L)
        );

        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        instance.toXContent(builder, ToXContent.EMPTY_PARAMS);

        assertThat(Strings.toString(builder), is(XContentHelper.stripWhitespace("""
            {
              "heuristics": {
                "properties": ["heuristic1", "heuristic2"],
                "status": "beta",
                "release_date": "2025-01-01",
                "end_of_life_date": "2025-12-31"
              },
              "internal": {
                "fingerprint": "fingerprint",
                "version": 1
              }
            }
            """)));
    }

    public void testFingerprintMatches() {
        var nullFingerprint1 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, null)
        );
        var nullFingerprint2 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, null)
        );
        var fingerprintAbc1 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal("abc", null)
        );
        var fingerprintAbc2 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal("abc", null)
        );
        var fingerprintXyz1 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null)
        );
        var fingerprintXyz2 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null)
        );

        assertTrue(nullFingerprint1.fingerprintMatches(nullFingerprint2));
        assertFalse(nullFingerprint1.fingerprintMatches(fingerprintAbc1));
        assertFalse(nullFingerprint1.fingerprintMatches(fingerprintXyz1));

        assertTrue(fingerprintAbc1.fingerprintMatches(fingerprintAbc2));
        assertTrue(fingerprintXyz1.fingerprintMatches(fingerprintXyz2));

        assertFalse(fingerprintXyz1.fingerprintMatches(fingerprintAbc1));
    }

    public void testIsNewerThan() {
        var nullVersion1 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, null)
        );
        var nullVersion2 = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, null)
        );
        var versionFour = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L)
        );
        var anotherVersionFour = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L)
        );
        var versionFive = new EndpointMetadataClusterState(
            EndpointMetadataTests.randomHeuristics(),
            new EndpointMetadata.Internal(null, 5L)
        );

        assertFalse(nullVersion1.isNewerThan(nullVersion2));
        assertFalse(nullVersion1.isNewerThan(versionFour));
        assertTrue(versionFour.isNewerThan(nullVersion1));
        assertFalse(versionFour.isNewerThan(anotherVersionFour));
        assertFalse(versionFour.isNewerThan(versionFive));
        assertTrue(versionFive.isNewerThan(versionFour));
        assertTrue(versionFive.isNewerThan(nullVersion2));
    }

    /**
     * Verifies that the lenient parser silently drops unknown fields written by older nodes (display, regions, denied_by_region_policy).
     */
    public void testParse_DropsUnknownFieldsFromFullEndpointMetadataJson() throws IOException {
        var expectedHeuristics = new EndpointMetadata.Heuristics(
            List.of("heuristic1", "heuristic2"),
            StatusHeuristic.BETA,
            "2025-01-01",
            "2025-12-31"
        );
        var expectedInternal = new EndpointMetadata.Internal("fingerprint", 1L);

        try (var parser = createParser(XContentType.JSON.xContent(), EndpointMetadataTests.NON_EMPTY_ENDPOINT_METADATA_JSON)) {
            var parsed = EndpointMetadataClusterState.parse(parser);
            assertThat(parsed.heuristics(), is(expectedHeuristics));
            assertThat(parsed.internal(), is(expectedInternal));
        }
    }

}
