/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.Connector;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.Split;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;

/**
 * With the {@code esql_satellites} feature flag off a server that implements the satellite protocol is read as a plain Flight server:
 * its capability action is never called, so no fragment is built for it, and a read still returns the rows. The flag is a
 * constructor argument of the factory, so the test does not depend on the build being a snapshot.
 */
public class FlightSatellitesFlagTests extends ESTestCase {

    private static final List<Attribute> ATTRIBUTES = List.of(referenceAttribute("id", DataType.INTEGER));

    private BlockFactory blockFactory;
    private BlockingSatelliteServer server;

    @Before
    public void startServer() throws Exception {
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build();
        server = new BlockingSatelliteServer();
    }

    @After
    public void stopServer() throws Exception {
        server.close();
        assertEquals("a client or stream was left open", 0, blockFactory.arrowAllocator().getChildAllocators().size());
        assertEquals("a page was left unreleased", 0L, blockFactory.arrowAllocator().getAllocatedMemory());
    }

    private FlightConnectorFactory factory(boolean satellites) {
        return new FlightConnectorFactory(
            TimeValue.timeValueSeconds(30),
            new SatelliteDeclarationCache(TimeValue.ZERO),
            FlightGuard.DEFAULT,
            satellites
        );
    }

    private String location() {
        return "flight://localhost:" + server.port() + "/t";
    }

    public void testWithTheFlagOnTheCapabilityIsReadAndStamped() {
        SourceMetadata metadata = factory(true).resolveMetadata(location(), Map.of());
        assertEquals(1, server.capabilityCalls.get());
        assertNotNull(metadata.sourceMetadata().get(SatelliteFragment.CAPABILITY_KEY));
    }

    public void testWithTheFlagOffTheServerIsAPlainOne() throws Exception {
        FlightConnectorFactory factory = factory(false);
        SourceMetadata metadata = factory.resolveMetadata(location(), Map.of());
        assertEquals("the capability action must not be called", 0, server.capabilityCalls.get());
        assertNull("no stamp, so no fragment is ever built", metadata.sourceMetadata().get(SatelliteFragment.CAPABILITY_KEY));
        assertEquals(1, metadata.schema().size());

        try (
            Connector connector = factory.open(Map.of("endpoint", "flight://localhost:" + server.port()));
            ResultCursor cursor = connector.execute(
                new QueryRequest("t", List.of("id"), ATTRIBUTES, Map.of(), 1000, blockFactory),
                Split.SINGLE
            )
        ) {
            int rows = 0;
            while (cursor.hasNext()) {
                Page page = cursor.next();
                rows += page.getPositionCount();
                page.releaseBlocks();
            }
            assertEquals(3, rows);
        }
        assertEquals("a plain read sends no fragment", 0, server.fragments.size());
        assertEquals(0, server.capabilityCalls.get());
    }

    public void testWithTheFlagOffASatelliteOnlySettingFailsTheQueryAndSaysWhy() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory(false).resolveMetadata(location(), Map.of("units", "metric"))
        );
        assertThat(e.getMessage(), containsString("satellites are disabled"));
        assertThat(e.getMessage(), containsString("esql_satellites"));
        assertEquals(0, server.capabilityCalls.get());
    }
}
