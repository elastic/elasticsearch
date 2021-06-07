/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DistanceUnitTests extends ESTestCase {
    public void testSimpleDistanceUnit() {
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.MILES), closeTo(16.09344, 0.001));
        assertThat(DistanceUnit.MILES.convert(10, DistanceUnit.MILES), closeTo(10, 0.001));
        assertThat(DistanceUnit.MILES.convert(10, DistanceUnit.KILOMETERS), closeTo(6.21371192, 0.001));
        assertThat(DistanceUnit.NAUTICALMILES.convert(10, DistanceUnit.MILES), closeTo(8.689762, 0.001));
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.KILOMETERS), closeTo(10, 0.001));
        assertThat(DistanceUnit.KILOMETERS.convert(10, DistanceUnit.METERS), closeTo(0.01, 0.00001));
        assertThat(DistanceUnit.KILOMETERS.convert(1000,DistanceUnit.METERS), closeTo(1, 0.001));
        assertThat(DistanceUnit.METERS.convert(1, DistanceUnit.KILOMETERS), closeTo(1000, 0.001));
    }

    public void testDistanceUnitParsing() {
        assertThat(DistanceUnit.Distance.parseDistance("50km").unit, equalTo(DistanceUnit.KILOMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("500m").unit, equalTo(DistanceUnit.METERS));
        assertThat(DistanceUnit.Distance.parseDistance("51mi").unit, equalTo(DistanceUnit.MILES));
        assertThat(DistanceUnit.Distance.parseDistance("53nmi").unit, equalTo(DistanceUnit.NAUTICALMILES));
        assertThat(DistanceUnit.Distance.parseDistance("53NM").unit, equalTo(DistanceUnit.NAUTICALMILES));
        assertThat(DistanceUnit.Distance.parseDistance("52yd").unit, equalTo(DistanceUnit.YARD));
        assertThat(DistanceUnit.Distance.parseDistance("12in").unit, equalTo(DistanceUnit.INCH));
        assertThat(DistanceUnit.Distance.parseDistance("23mm").unit, equalTo(DistanceUnit.MILLIMETERS));
        assertThat(DistanceUnit.Distance.parseDistance("23cm").unit, equalTo(DistanceUnit.CENTIMETERS));

        double testValue = 12345.678;
        for (DistanceUnit unit : DistanceUnit.values()) {
            assertThat("Unit can be parsed from '" + unit.toString() + "'", DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Unit can be parsed from '" + testValue + unit.toString() + "'",
                DistanceUnit.fromString(unit.toString()), equalTo(unit));
            assertThat("Value can be parsed from '" + testValue + unit.toString() + "'",
                DistanceUnit.Distance.parseDistance(unit.toString(testValue)).value, equalTo(testValue));
        }
    }

    /**
     * This test ensures that we are aware of accidental reordering in the distance unit ordinals,
     * since equality in e.g. CircleShapeBuilder, hashCode and serialization rely on them
     */
    public void testDistanceUnitNames() {
        assertEquals(0, DistanceUnit.INCH.ordinal());
        assertEquals(1, DistanceUnit.YARD.ordinal());
        assertEquals(2, DistanceUnit.FEET.ordinal());
        assertEquals(3, DistanceUnit.KILOMETERS.ordinal());
        assertEquals(4, DistanceUnit.NAUTICALMILES.ordinal());
        assertEquals(5, DistanceUnit.MILLIMETERS.ordinal());
        assertEquals(6, DistanceUnit.CENTIMETERS.ordinal());
        assertEquals(7, DistanceUnit.MILES.ordinal());
        assertEquals(8, DistanceUnit.METERS.ordinal());
    }

    public void testReadWrite() throws Exception {
        for (DistanceUnit unit : DistanceUnit.values()) {
          try (BytesStreamOutput out = new BytesStreamOutput()) {
              unit.writeTo(out);
              try (StreamInput in = out.bytes().streamInput()) {
                  assertThat("Roundtrip serialisation failed.", DistanceUnit.readFromStream(in), equalTo(unit));
              }
          }
        }
    }

    public void testFromString() {
        for (DistanceUnit unit : DistanceUnit.values()) {
            assertThat("Roundtrip string parsing failed.", DistanceUnit.fromString(unit.toString()), equalTo(unit));
        }
    }
}
