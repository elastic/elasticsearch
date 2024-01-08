/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

public class UnitOfMeasurementTests extends ESTestCase {

    public void testNullUnit() {
        UnitOfMeasurement unit = UnitOfMeasurement.of(null);
        assertEquals("42thrd", unit.tryConvert("42thrd", "jvm_thread_count"));
    }

    public void testUnknownUnit() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("{threads}");
        assertEquals("42thrd", unit.tryConvert("42thrd", "jvm_thread_count"));
    }

    public void testByteUnit() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("By");
        assertEquals(42, unit.tryConvert(42, "request_bytes"));
        assertEquals("42", unit.tryConvert("42", "request_bytes"));
        assertEquals(42L, unit.tryConvert("42b ", "request_bytes"));
        assertEquals(42L * 1024, unit.tryConvert("42 KB", "request_bytes"));
    }

    public void testFractionalBytes() {
        assertEquals((long) (1.1 * 1024), UnitOfMeasurement.of("By").tryConvert("1.1kb", "request_bytes"));
        assertWarnings(
            "Fractional bytes values are deprecated. Use non-fractional bytes values instead: [1.1kb] found for [request_bytes]"
        );
    }

    public void testByteUnitInvalid() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("byte");
        assertEquals(
            "failed to parse [request_bytes] with value [-42b] as a size in bytes",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("-42b", "request_bytes")).getMessage()
        );
        assertEquals(
            "failed to parse [request_bytes] with value [1by] as a size in bytes: unit is missing or unrecognized",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("1by", "request_bytes")).getMessage()
        );
        assertEquals(
            "failed to parse [request_bytes] with value [1foo] as a size in bytes: unit is missing or unrecognized",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("1foo", "request_bytes")).getMessage()
        );
    }

    public void testTimeUnit() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("ns");
        assertEquals(42, unit.tryConvert(42, "duration"));
        assertEquals("42", unit.tryConvert("42", "duration"));
        assertEquals(42L, unit.tryConvert("42 ns", "duration"));
        assertEquals(42L * 1000, unit.tryConvert("42US ", "duration"));
    }

    public void testTimeUnitInvalid() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("s");
        assertEquals(
            "failed to parse [duration] with value [-42s] as a time value: negative durations are not supported",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("-42s", "duration")).getMessage()
        );
        assertEquals(
            "failed to parse [duration] with value [1sec] as a time value: unit is missing or unrecognized",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("1sec", "duration")).getMessage()
        );
        assertEquals(
            "failed to parse [duration] with value [1foo] as a time value: unit is missing or unrecognized",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("1foo", "duration")).getMessage()
        );
        assertEquals(
            "failed to parse [1.1s], fractional time values are not supported",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("1.1s", "duration")).getMessage()
        );
    }

    public void testPercent() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("%");
        assertEquals(42, unit.tryConvert(42, "ratio"));
        assertEquals("42", unit.tryConvert("42", "ratio"));
        assertEquals(0.42, unit.tryConvert("42%", "ratio"));
        assertEquals(0.421, (double) unit.tryConvert("42.1%", "ratio"), 0.000001);
    }

    public void testPercentInvalid() {
        UnitOfMeasurement unit = UnitOfMeasurement.of("percent");
        assertEquals(
            "Percentage should be in [0-100], got [200]",
            expectThrows(ElasticsearchParseException.class, () -> unit.tryConvert("200%", "effort")).getMessage()
        );
    }
}
