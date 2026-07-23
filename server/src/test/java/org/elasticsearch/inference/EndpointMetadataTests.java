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
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

public class EndpointMetadataTests extends AbstractBWCSerializationTestCase<EndpointMetadata> {

    private static final EndpointMetadata NON_EMPTY_ENDPOINT_METADATA = new EndpointMetadata(
        new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
        new EndpointMetadata.Internal("fingerprint", 1L),
        new EndpointMetadata.Display("name", "some_creator"),
        List.of(new EndpointMetadata.EndpointRegion("aws", "us-east-1", "us")),
        true
    );

    private static final String NON_EMPTY_ENDPOINT_METADATA_JSON = """
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
          },
          "display": {
            "name": "name",
            "model_creator": "some_creator"
          },
          "regions": [{"csp": "aws", "region": "us-east-1", "geo": "us"}],
          "denied_by_region_policy": true
        }
        """;

    private static final String NON_EMPTY_ENDPOINT_METADATA_JSON_WITHOUT_INTERNAL = """
        {
          "heuristics": {
            "properties": ["heuristic1", "heuristic2"],
            "status": "beta",
            "release_date": "2025-01-01",
            "end_of_life_date": "2025-12-31"
          },
          "display": {
            "name": "name",
            "model_creator": "some_creator"
          },
          "regions": [{"csp": "aws", "region": "us-east-1", "geo": "us"}],
          "denied_by_region_policy": true
        }
        """;

    public static EndpointMetadata randomInstance() {
        if (randomBoolean()) {
            return EndpointMetadata.EMPTY_INSTANCE;
        }

        var heuristics = randomHeuristics();
        var internal = randomInternal();
        var display = randomDisplay();
        var regions = randomRegions();
        var deniedByRegionPolicy = randomBoolean();

        var instance = new EndpointMetadata(heuristics, internal, display, regions, deniedByRegionPolicy);
        return EndpointMetadata.EMPTY_INSTANCE.equals(instance) ? EndpointMetadata.EMPTY_INSTANCE : instance;
    }

    public static EndpointMetadata randomNonEmptyInstance() {
        var properties = IntStream.range(1, randomIntBetween(2, 5))
            .mapToObj(i -> randomAlphaOfLength(randomIntBetween(1, 10)))
            .collect(Collectors.toList());
        var status = randomFrom(StatusHeuristic.values());
        var releaseDate = randomLocalDate();
        var endOfLifeDate = randomLocalDate();
        var heuristics = new EndpointMetadata.Heuristics(properties, status, releaseDate, endOfLifeDate);

        var fingerprint = randomAlphaOfLengthBetween(10, 50);
        var version = randomLongBetween(0, Long.MAX_VALUE);
        var internal = new EndpointMetadata.Internal(fingerprint, version);

        var display = new EndpointMetadata.Display(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLength(10));

        var regions = IntStream.range(0, randomIntBetween(1, 3)).mapToObj(i -> randomEndpointRegion()).collect(Collectors.toList());

        return new EndpointMetadata(heuristics, internal, display, regions, randomBoolean());
    }

    public static EndpointMetadata.EndpointRegion randomEndpointRegion() {
        return new EndpointMetadata.EndpointRegion(
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 10),
            randomBoolean() ? null : randomAlphaOfLengthBetween(3, 15),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 5)
        );
    }

    public static List<EndpointMetadata.EndpointRegion> randomRegions() {
        if (randomBoolean()) {
            return List.of();
        }
        return IntStream.range(0, randomIntBetween(1, 3)).mapToObj(i -> randomEndpointRegion()).collect(Collectors.toList());
    }

    public static EndpointMetadata.Display randomDisplay() {
        return randomBoolean()
            ? EndpointMetadata.Display.EMPTY_INSTANCE
            : new EndpointMetadata.Display(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLength(10));
    }

    public static EndpointMetadata.Heuristics randomHeuristics() {
        if (randomBoolean()) {
            return EndpointMetadata.Heuristics.EMPTY_INSTANCE;
        }

        var properties = IntStream.range(0, randomIntBetween(0, 5))
            .mapToObj(i -> randomAlphaOfLength(randomIntBetween(1, 10)))
            .collect(Collectors.toList());
        var status = randomBoolean() ? null : randomFrom(StatusHeuristic.values());
        var releaseDate = randomBoolean() ? null : randomLocalDate();
        var endOfLifeDate = randomBoolean() ? null : randomLocalDate();

        var instance = new EndpointMetadata.Heuristics(properties, status, releaseDate, endOfLifeDate);
        return EndpointMetadata.Heuristics.EMPTY_INSTANCE.equals(instance) ? EndpointMetadata.Heuristics.EMPTY_INSTANCE : instance;
    }

    private static LocalDate randomLocalDate() {
        var minDay = LocalDate.MIN.toEpochDay();
        var maxDay = LocalDate.now(ZoneId.systemDefault()).toEpochDay();
        return LocalDate.ofEpochDay(randomLongBetween(minDay, maxDay));
    }

    public static EndpointMetadata.Internal randomInternal() {
        if (randomBoolean()) {
            return EndpointMetadata.Internal.EMPTY_INSTANCE;
        }

        var fingerprint = randomBoolean() ? null : randomAlphaOfLengthBetween(10, 50);
        var version = randomBoolean() ? null : randomLongBetween(0, Long.MAX_VALUE);

        var instance = new EndpointMetadata.Internal(fingerprint, version);
        return EndpointMetadata.Internal.EMPTY_INSTANCE.equals(instance) ? EndpointMetadata.Internal.EMPTY_INSTANCE : instance;
    }

    public void testToXContentEmptyEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        EndpointMetadata.EMPTY_INSTANCE.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace("""
            {
              "heuristics": {
                "properties": []
              },
              "internal": {},
              "display": {}
            }
            """)));
    }

    public void testToXContentNonEmptyEndpointMetadata() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        NON_EMPTY_ENDPOINT_METADATA.toXContent(builder, ToXContent.EMPTY_PARAMS);
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(NON_EMPTY_ENDPOINT_METADATA_JSON)));
    }

    public void testToXContentExcludesInternalWhenParamSet() throws IOException {
        var builder = XContentFactory.contentBuilder(XContentType.JSON);
        NON_EMPTY_ENDPOINT_METADATA.toXContent(builder, NON_EMPTY_ENDPOINT_METADATA.getXContentParamsExcludeInternalFields());
        var json = Strings.toString(builder);

        assertThat(json, is(XContentHelper.stripWhitespace(NON_EMPTY_ENDPOINT_METADATA_JSON_WITHOUT_INTERNAL)));
    }

    public void testFingerprintMatches() {
        EndpointMetadata endpointWithNullFingerprint1 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithNullFingerprint2 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintAbc1 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal("abc", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintAbc2 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal("abc", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintXyz1 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintXyz2 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null),
            randomDisplay(),
            List.of(),
            false
        );

        assertThat(endpointWithNullFingerprint1.fingerprintMatches(endpointWithNullFingerprint2), is(true));
        assertThat(endpointWithNullFingerprint1.fingerprintMatches(endpointWithFingerprintAbc1), is(false));
        assertThat(endpointWithNullFingerprint1.fingerprintMatches(endpointWithFingerprintXyz1), is(false));

        assertThat(endpointWithFingerprintAbc1.fingerprintMatches(endpointWithFingerprintAbc2), is(true));
        assertThat(endpointWithFingerprintXyz1.fingerprintMatches(endpointWithFingerprintXyz2), is(true));

        assertThat(endpointWithFingerprintXyz1.fingerprintMatches(endpointWithFingerprintAbc1), is(false));
    }

    public void testHasNewerVersionThan() {
        EndpointMetadata endpointWithNullVersion1 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithNullVersion2 = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithVersionFour = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata anotherEndpointWithVersionFour = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithVersionFive = new EndpointMetadata(
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 5L),
            randomDisplay(),
            List.of(),
            false
        );

        assertThat(endpointWithNullVersion1.hasNewerVersionThan(endpointWithNullVersion2), is(false));
        assertThat(endpointWithNullVersion1.hasNewerVersionThan(endpointWithVersionFour), is(false));
        assertThat(endpointWithVersionFour.hasNewerVersionThan(endpointWithNullVersion1), is(true));
        assertThat(endpointWithVersionFour.hasNewerVersionThan(anotherEndpointWithVersionFour), is(false));
        assertThat(endpointWithVersionFour.hasNewerVersionThan(endpointWithVersionFive), is(false));
        assertThat(endpointWithVersionFive.hasNewerVersionThan(endpointWithVersionFour), is(true));
        assertThat(endpointWithVersionFive.hasNewerVersionThan(endpointWithNullVersion2), is(true));
    }

    @Override
    protected EndpointMetadata createTestInstance() {
        return randomInstance();
    }

    @Override
    protected EndpointMetadata doParseInstance(XContentParser parser) throws IOException {
        return EndpointMetadata.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Writeable.Reader<EndpointMetadata> instanceReader() {
        return EndpointMetadata::new;
    }

    @Override
    protected EndpointMetadata mutateInstance(EndpointMetadata instance) throws IOException {
        var heuristics = instance.heuristics();
        var internal = instance.internal();
        var display = instance.display();
        var regions = instance.regions();
        var deniedByRegionPolicy = instance.deniedByRegionPolicy();

        switch (randomInt(4)) {
            case 0 -> heuristics = randomValueOtherThan(heuristics, EndpointMetadataTests::randomHeuristics);
            case 1 -> internal = randomValueOtherThan(internal, EndpointMetadataTests::randomInternal);
            case 2 -> display = randomValueOtherThan(display, EndpointMetadataTests::randomDisplay);
            case 3 -> regions = randomValueOtherThan(regions, EndpointMetadataTests::randomRegions);
            case 4 -> deniedByRegionPolicy = deniedByRegionPolicy == false;
        }

        return new EndpointMetadata(heuristics, internal, display, regions, deniedByRegionPolicy);
    }

    @Override
    protected EndpointMetadata mutateInstanceForVersion(EndpointMetadata instance, TransportVersion version) {
        return doMutateInstanceForVersion(instance, version);
    }

    public static EndpointMetadata doMutateInstanceForVersion(EndpointMetadata instance, TransportVersion version) {
        var heuristics = instance.heuristics();
        var internal = instance.internal();
        var display = instance.display();
        var regions = instance.regions();
        var deniedByRegionPolicy = instance.deniedByRegionPolicy();

        if (version.supports(EndpointMetadata.Display.MODEL_CREATOR_ADDED) == false) {
            display = new EndpointMetadata.Display(display.name(), null);
        }
        if (version.supports(EndpointMetadata.REGIONS_ADDED) == false) {
            regions = List.of();
            deniedByRegionPolicy = false;
        }
        return new EndpointMetadata(heuristics, internal, display, regions, deniedByRegionPolicy);
    }
}
