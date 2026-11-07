/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;

/**
 * The satellite capability stamp rides in the {@code sourceMetadata} of the plan, which is written to the data nodes as a generic
 * value. The stamp holds the whole declaration, unknown properties included (a consumer of a declaration must ignore them, not
 * drop them), so whatever JSON a satellite sends has to be writable, or the plan fails when it is sent and not when it is resolved.
 */
public class FlightStampSerializationTests extends ESTestCase {

    private static final String DECLARATION = "{\"specVersions\":[0],\"fragmentFormats\":[\"json\"],\"plan\":[\"scan\",\"limit\"],"
        + "\"functions\":[],\"types\":[\"integer\"],\"options\":[\"units\"]";

    private BlockingSatelliteServer server;

    @Before
    public void startServer() throws Exception {
        server = new BlockingSatelliteServer();
    }

    @After
    public void stopServer() throws Exception {
        server.close();
    }

    private Map<String, Object> resolveStamp() {
        FlightConnectorFactory factory = new FlightConnectorFactory(
            TimeValue.timeValueSeconds(30),
            new SatelliteDeclarationCache(TimeValue.ZERO),
            FlightGuard.DEFAULT,
            true
        );
        SourceMetadata metadata = factory.resolveMetadata("flight://localhost:" + server.port() + "/t", Map.of());
        assertThat(metadata.sourceMetadata().get(SatelliteFragment.CAPABILITY_KEY), notNullValue());
        return metadata.sourceMetadata();
    }

    private static void assertRoundTrip(Map<String, Object> sourceMetadata) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(sourceMetadata);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(sourceMetadata, in.readGenericValue());
            }
        }
    }

    public void testTheStampOfADeclarationSurvivesTheWire() throws IOException {
        assertRoundTrip(resolveStamp());
    }

    public void testUnknownPropertiesOfEveryJsonTypeSurviveTheWire() throws IOException {
        server.capabilities = DECLARATION
            + ",\"x-vendor\":{\"int\":1,\"long\":12345678901,\"double\":1.5,\"bool\":true,\"text\":\"a\",\"list\":[1,2],\"none\":null}}";
        assertRoundTrip(resolveStamp());
    }

    public void testAnIntegerBeyondALongInAnUnknownPropertyDoesNotBreakThePlan() throws IOException {
        server.capabilities = DECLARATION + ",\"x-vendor\":{\"huge\":123456789012345678901234567890}}";
        Map<String, Object> stamp = resolveStamp();
        Map<?, ?> declaration = (Map<?, ?>) stamp.get(SatelliteFragment.CAPABILITY_KEY);
        assertThat(
            "the unknown property is kept in the stamp, so it is written with the plan",
            declaration.get("x-vendor"),
            notNullValue()
        );
        assertRoundTrip(stamp);
    }
}
