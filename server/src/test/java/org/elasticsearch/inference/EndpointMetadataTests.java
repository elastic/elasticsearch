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
        new EndpointMetadata.ModelIdentity("elastic", "elser", null, "v2"),
        new EndpointMetadata.Heuristics(List.of("heuristic1", "heuristic2"), StatusHeuristic.BETA, "2025-01-01", "2025-12-31"),
        new EndpointMetadata.Internal("fingerprint", 1L),
        new EndpointMetadata.Display("name", "some_creator"),
        List.of(new EndpointMetadata.EndpointRegion("aws", "us-east-1", "us", "US East (N. Virginia)")),
        true
    );

    static final String NON_EMPTY_ENDPOINT_METADATA_JSON = """
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
          "model_identity": {
            "creator": "elastic",
            "family": "elser",
            "version": "v2"
          },
          "regions": [{"csp": "aws", "region": "us-east-1", "geo": "us", "region_display_name": "US East (N. Virginia)"}],
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
          "model_identity": {
            "creator": "elastic",
            "family": "elser",
            "version": "v2"
          },
          "regions": [{"csp": "aws", "region": "us-east-1", "geo": "us", "region_display_name": "US East (N. Virginia)"}],
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
        var modelIdentity = randomModelIdentity();
        var regions = randomRegions();
        var deniedByRegionPolicy = randomBoolean();

        var instance = new EndpointMetadata(modelIdentity, heuristics, internal, display, regions, deniedByRegionPolicy);
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
        var modelIdentity = randomModelIdentity();
        var regions = IntStream.range(0, randomIntBetween(1, 3)).mapToObj(i -> randomEndpointRegion()).collect(Collectors.toList());
        return new EndpointMetadata(modelIdentity, heuristics, internal, display, regions, randomBoolean());
    }

    public static EndpointMetadata.EndpointRegion randomEndpointRegion() {
        return new EndpointMetadata.EndpointRegion(
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 10),
            randomBoolean() ? null : randomAlphaOfLengthBetween(3, 15),
            randomBoolean() ? null : randomAlphaOfLengthBetween(2, 5),
            randomBoolean() ? null : randomAlphaOfLengthBetween(5, 30)
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

    public static EndpointMetadata.ModelIdentity randomModelIdentity() {
        if (randomBoolean()) {
            return EndpointMetadata.ModelIdentity.EMPTY_INSTANCE;
        }

        var creator = randomBoolean() ? null : randomAlphaOfLengthBetween(2, 15);
        var family = randomBoolean() ? null : randomAlphaOfLengthBetween(2, 15);
        var tier = randomBoolean() ? null : randomAlphaOfLengthBetween(2, 10);
        var version = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10);

        var instance = new EndpointMetadata.ModelIdentity(creator, family, tier, version);
        return EndpointMetadata.ModelIdentity.EMPTY_INSTANCE.equals(instance) ? EndpointMetadata.ModelIdentity.EMPTY_INSTANCE : instance;
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
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithNullFingerprint2 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintAbc1 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal("abc", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintAbc2 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal("abc", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintXyz1 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithFingerprintXyz2 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal("xyz", null),
            randomDisplay(),
            List.of(),
            false
        );

        assertTrue(fingerprintMatches(endpointWithNullFingerprint1, endpointWithNullFingerprint2));
        assertFalse(fingerprintMatches(endpointWithNullFingerprint1, endpointWithFingerprintAbc1));
        assertFalse(fingerprintMatches(endpointWithNullFingerprint1, endpointWithFingerprintXyz1));

        assertTrue(fingerprintMatches(endpointWithFingerprintAbc1, endpointWithFingerprintAbc2));
        assertTrue(fingerprintMatches(endpointWithFingerprintXyz1, endpointWithFingerprintXyz2));

        assertFalse(fingerprintMatches(endpointWithFingerprintXyz1, endpointWithFingerprintAbc1));
    }

    public void testIsNewerThan() {
        EndpointMetadata endpointWithNullVersion1 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithNullVersion2 = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, null),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithVersionFour = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata anotherEndpointWithVersionFour = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 4L),
            randomDisplay(),
            List.of(),
            false
        );
        EndpointMetadata endpointWithVersionFive = new EndpointMetadata(
            randomModelIdentity(),
            randomHeuristics(),
            new EndpointMetadata.Internal(null, 5L),
            randomDisplay(),
            List.of(),
            false
        );

        assertFalse(isNewerThan(endpointWithNullVersion1, endpointWithNullVersion2));
        assertFalse(isNewerThan(endpointWithNullVersion1, endpointWithVersionFour));
        assertTrue(isNewerThan(endpointWithVersionFour, endpointWithNullVersion1));
        assertFalse(isNewerThan(endpointWithVersionFour, anotherEndpointWithVersionFour));
        assertFalse(isNewerThan(endpointWithVersionFour, endpointWithVersionFive));
        assertTrue(isNewerThan(endpointWithVersionFive, endpointWithVersionFour));
        assertTrue(isNewerThan(endpointWithVersionFive, endpointWithNullVersion2));
    }

    private static boolean fingerprintMatches(EndpointMetadata first, EndpointMetadata second) {
        return first.internal().fingerprintMatches(second.internal());
    }

    private static boolean isNewerThan(EndpointMetadata first, EndpointMetadata second) {
        return first.internal().isNewerThan(second.internal());
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
        var modelIdentity = instance.modelIdentity();
        var regions = instance.regions();
        var deniedByRegionPolicy = instance.deniedByRegionPolicy();

        switch (randomInt(5)) {
            case 0 -> heuristics = randomValueOtherThan(heuristics, EndpointMetadataTests::randomHeuristics);
            case 1 -> internal = randomValueOtherThan(internal, EndpointMetadataTests::randomInternal);
            case 2 -> display = randomValueOtherThan(display, EndpointMetadataTests::randomDisplay);
            case 3 -> modelIdentity = randomValueOtherThan(modelIdentity, EndpointMetadataTests::randomModelIdentity);
            case 4 -> regions = randomValueOtherThan(regions, EndpointMetadataTests::randomRegions);
            case 5 -> deniedByRegionPolicy = deniedByRegionPolicy == false;
        }

        return new EndpointMetadata(modelIdentity, heuristics, internal, display, regions, deniedByRegionPolicy);
    }

    @Override
    protected EndpointMetadata mutateInstanceForVersion(EndpointMetadata instance, TransportVersion version) {
        return doMutateInstanceForVersion(instance, version);
    }

    public static EndpointMetadata doMutateInstanceForVersion(EndpointMetadata instance, TransportVersion version) {
        var heuristics = instance.heuristics();
        var internal = instance.internal();
        var display = instance.display();
        var modelIdentity = instance.modelIdentity();
        var regions = instance.regions();
        var deniedByRegionPolicy = instance.deniedByRegionPolicy();

        if (version.supports(EndpointMetadata.Display.MODEL_CREATOR_ADDED) == false) {
            display = new EndpointMetadata.Display(display.name(), null);
        }
        if (version.supports(EndpointMetadata.ModelIdentity.MODEL_IDENTITY_ADDED) == false) {
            modelIdentity = EndpointMetadata.ModelIdentity.EMPTY_INSTANCE;
        }
        if (version.supports(EndpointMetadata.REGIONS_ADDED) == false) {
            regions = List.of();
            deniedByRegionPolicy = false;
        } else if (version.supports(EndpointMetadata.EndpointRegion.REGION_DISPLAY_NAME_ADDED) == false) {
            regions = regions.stream()
                .map(r -> new EndpointMetadata.EndpointRegion(r.csp(), r.region(), r.geo(), null))
                .collect(Collectors.toList());
        }
        return new EndpointMetadata(modelIdentity, heuristics, internal, display, regions, deniedByRegionPolicy);
    }
}
