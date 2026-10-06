/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;

import java.util.List;
import java.util.Map;

public class ReadDecisionTests extends ESTestCase {

    private static Attribute column(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    /**
     * The lanes a decision keys on and the hex a record's metadata carries must come from one encoder, or a
     * harvest's stamp and the address it is filed under could describe the same read differently.
     */
    public void testLanesAndHexDescribeTheSameRead() {
        List<Attribute> schema = List.of(column("a", DataType.LONG), column("b", DataType.KEYWORD));
        ReadDecision decision = ReadDecision.of(schema, DeclaredReadSpec.NONE);
        assertTrue(decision.isKnown());
        assertEquals(ReadConfigFingerprint.of(schema, DeclaredReadSpec.NONE), decision.toFingerprint());
        assertEquals(decision, ReadDecision.fromFingerprint(decision.toFingerprint()));
    }

    /** A different resolved read must derive a different address, which is the whole point of the component. */
    public void testARetypedColumnIsADifferentDecision() {
        ReadDecision asLong = ReadDecision.of(List.of(column("a", DataType.LONG)), DeclaredReadSpec.NONE);
        ReadDecision asKeyword = ReadDecision.of(List.of(column("a", DataType.KEYWORD)), DeclaredReadSpec.NONE);
        assertTrue(asLong.isKnown());
        assertNotEquals(asLong, asKeyword);
    }

    /**
     * The sentinels are a named kind precisely so no real hash can ever equal one. Asserting they differ from a
     * real decision is not enough - a reserved lane pair would pass that and still collide one day - so this
     * pins that they are not KNOWN at all.
     */
    public void testSentinelsAreNotAKnownRead() {
        assertFalse(ReadDecision.UNKNOWN.isKnown());
        assertFalse(ReadDecision.MIXED.isKnown());
        assertNotEquals(ReadDecision.UNKNOWN, ReadDecision.MIXED);
        assertEquals(ReadDecision.UNKNOWN, ReadDecision.of(List.of(), DeclaredReadSpec.NONE));
        assertEquals(ReadDecision.UNKNOWN, ReadDecision.of(null, DeclaredReadSpec.NONE));
        assertEquals(ReadConfigFingerprint.UNKNOWN, ReadDecision.UNKNOWN.toFingerprint());
        assertEquals(ReadConfigFingerprint.MIXED, ReadDecision.MIXED.toFingerprint());
    }

    /** A fold's MIXED stamp must read back as MIXED, not as a 32-character hash and not as UNKNOWN. */
    public void testMixedAndMalformedStampsReadBack() {
        assertEquals(ReadDecision.MIXED, ReadDecision.fromFingerprint(ReadConfigFingerprint.MIXED));
        assertEquals(ReadDecision.UNKNOWN, ReadDecision.fromFingerprint(null));
        assertEquals(ReadDecision.UNKNOWN, ReadDecision.fromFingerprint(""));
        // Wrong length cannot have come from of(): known-and-never-matching, so the gate strips rather than serves.
        assertEquals(ReadDecision.MIXED, ReadDecision.fromFingerprint("deadbeef"));
    }

    /** Lane order must survive the hex round trip: swapping the halves is the characteristic off-by-one here. */
    public void testHighAndLowLanesDoNotSwap() {
        ReadDecision decision = new ReadDecision(ReadDecision.Kind.KNOWN, 0x0123456789abcdefL, 0x1L);
        assertEquals("0123456789abcdef0000000000000001", decision.toFingerprint());
        ReadDecision roundTripped = ReadDecision.fromFingerprint(decision.toFingerprint());
        assertEquals(0x0123456789abcdefL, roundTripped.high());
        assertEquals(0x1L, roundTripped.low());
    }
}
