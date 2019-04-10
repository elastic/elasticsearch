/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;

import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;
import static org.hamcrest.Matchers.instanceOf;

public class StDistanceProcessorTests extends AbstractWireSerializingTestCase<StDistanceProcessor> {

    public StDistanceProcessor createTestInstance() {
        return new StDistanceProcessor(
            constantPoint(randomDoubleBetween(-180, 180, true), randomDoubleBetween(-90, 90, true)),
            constantPoint(randomDoubleBetween(-180, 180, true), randomDoubleBetween(-90, 90, true))
        );
    }

    public static Processor constantPoint(double lon, double lat) {
        return new ChainingProcessor(new ConstantProcessor("point (" + lon + " " + lat + ")"), StWkttosqlProcessor.INSTANCE);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testApply() {
        StDistanceProcessor proc = new StDistanceProcessor(constantPoint(10, 20), constantPoint(30, 40));
        Object result = proc.process(null);
        assertThat(result, instanceOf(Double.class));
        assertEquals(GeoUtils.arcDistance(20, 10, 40, 30), (double) result, 0.000001);
    }

    public void testNullHandling() {
        assertNull(new StDistance(EMPTY, l(new GeoShape(1, 2)), l(null)).makePipe().asProcessor().process(null));
        assertNull(new StDistance(EMPTY, l(null), l(new GeoShape(1, 2))).makePipe().asProcessor().process(null));
    }

    public void testTypeCheck() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new StDistance(EMPTY, l("foo"), l(new GeoShape(1, 2))).makePipe().asProcessor().process(null));
        assertEquals("A geo_point or geo_shape with type point is required; received [foo]", siae.getMessage());

        siae = expectThrows(SqlIllegalArgumentException.class,
            () -> new StDistance(EMPTY, l(new GeoShape(1, 2)), l("bar")).makePipe().asProcessor().process(null));
        assertEquals("A geo_point or geo_shape with type point is required; received [bar]", siae.getMessage());
    }

    @Override
    protected Writeable.Reader<StDistanceProcessor> instanceReader() {
        return StDistanceProcessor::new;
    }
}
