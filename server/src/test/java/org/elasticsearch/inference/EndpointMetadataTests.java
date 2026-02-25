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
        new EndpointMetadata.Display("name")
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
            "name": "name"
          }
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
            "name": "name"
          }
        }
        """;

    public static EndpointMetadata randomInstance() {
        if (randomBoolean()) {
            return EndpointMetadata.EMPTY_INSTANCE;
        }

        var heuristics = randomHeuristics();
        var internal = randomInternal();
        var display = randomDisplay();

        if (InferenceFieldUtils.isNull(heuristics, internal, display)) {
            return EndpointMetadata.EMPTY_INSTANCE;
        }
        return new EndpointMetadata(heuristics, internal, display);
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

        var display = new EndpointMetadata.Display(randomAlphaOfLengthBetween(1, 20));

        return new EndpointMetadata(heuristics, internal, display);
    }

    public static EndpointMetadata.Display randomDisplay() {
        return randomBoolean() ? EndpointMetadata.Display.EMPTY_INSTANCE : new EndpointMetadata.Display(randomAlphaOfLengthBetween(1, 20));
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

        if (InferenceFieldUtils.isNull(properties, status, releaseDate, endOfLifeDate)) {
            return EndpointMetadata.Heuristics.EMPTY_INSTANCE;
        }
        return new EndpointMetadata.Heuristics(properties, status, releaseDate, endOfLifeDate);
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

        if (InferenceFieldUtils.isNull(fingerprint, version)) {
            return EndpointMetadata.Internal.EMPTY_INSTANCE;
        }
        return new EndpointMetadata.Internal(fingerprint, version);
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

        switch (randomInt(2)) {
            case 0 -> heuristics = randomValueOtherThan(heuristics, EndpointMetadataTests::randomHeuristics);
            case 1 -> internal = randomValueOtherThan(internal, EndpointMetadataTests::randomInternal);
            case 2 -> display = randomValueOtherThan(display, EndpointMetadataTests::randomDisplay);
        }

        return new EndpointMetadata(heuristics, internal, display);
    }

    @Override
    protected EndpointMetadata mutateInstanceForVersion(EndpointMetadata instance, TransportVersion version) {
        return instance;
    }
}
