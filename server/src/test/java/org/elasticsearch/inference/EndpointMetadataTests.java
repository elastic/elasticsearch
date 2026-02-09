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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EndpointMetadataTests extends AbstractBWCSerializationTestCase<EndpointMetadata> {

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
