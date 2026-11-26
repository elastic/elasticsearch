/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for the resolved {@link EqlRelation} leaf: its accessors, the {@code with*} copy methods, the
 * equals/hashCode contract (which deliberately ignores the coordinator-local {@code preResolvedFieldCaps} carrier and
 * the node's {@code Source}), and the coordinator-local serialization contract — the node is never shipped to a data
 * node, so {@code writeTo}/{@code getWriteableName} throw rather than encode anything.
 */
public class EqlRelationTests extends ESTestCase {

    private static final IndexPattern PATTERN = new IndexPattern(EMPTY, "logs-*");
    private static final String QUERY = "sequence [process where true] [network where true] | tail 3";
    private static final Map<String, Object> OPTIONS = Map.of("size", 10);
    private static final List<Attribute> OUTPUT = List.of(new ReferenceAttribute(EMPTY, "@timestamp", DATETIME));

    private static EqlRelation relation() {
        return new EqlRelation(EMPTY, PATTERN, QUERY, OPTIONS, EqlRelation.Mode.SEQUENCE, true, OUTPUT, 5, fieldCaps());
    }

    private static FieldCapabilitiesResponse fieldCaps() {
        return new FieldCapabilitiesResponse(new String[] { "logs-1" }, Map.of());
    }

    public void testAccessors() {
        EqlRelation relation = relation();
        assertThat(relation.indexPattern(), sameInstance(PATTERN));
        assertThat(relation.query(), equalTo(QUERY));
        assertThat(relation.options(), equalTo(OPTIONS));
        assertThat(relation.mode(), equalTo(EqlRelation.Mode.SEQUENCE));
        assertTrue(relation.hasExplicitLimit());
        assertThat(relation.output(), equalTo(OUTPUT));
        assertThat(relation.pushedLimit(), equalTo(5));
        assertThat(relation.preResolvedFieldCaps(), not(equalTo(null)));
        assertTrue("EqlRelation exposes a fully resolved schema", relation.expressionsResolved());
    }

    public void testConvenienceConstructorLeavesOptionalFieldsNull() {
        EqlRelation relation = new EqlRelation(EMPTY, PATTERN, QUERY, OPTIONS, EqlRelation.Mode.SEQUENCE, true, OUTPUT);
        assertThat(relation.pushedLimit(), equalTo(null));
        assertThat(relation.preResolvedFieldCaps(), equalTo(null));
    }

    public void testWithPushedLimitReplacesLimitAndKeepsFieldCaps() {
        EqlRelation relation = relation();
        EqlRelation withLimit = relation.withPushedLimit(42);
        assertThat(withLimit.pushedLimit(), equalTo(42));
        // The carrier rides along so a later planning stage still reuses the resolved field-caps.
        assertThat(withLimit.preResolvedFieldCaps(), sameInstance(relation.preResolvedFieldCaps()));
        assertThat(withLimit.query(), equalTo(QUERY));
        assertThat(withLimit.mode(), equalTo(EqlRelation.Mode.SEQUENCE));
        assertTrue(withLimit.hasExplicitLimit());
    }

    public void testWithAttributesReplacesOutput() {
        List<Attribute> newOutput = List.of(
            new ReferenceAttribute(EMPTY, "@timestamp", DATETIME),
            new ReferenceAttribute(EMPTY, "foo", KEYWORD)
        );
        EqlRelation withAttrs = relation().withAttributes(newOutput);
        assertThat(withAttrs.output(), equalTo(newOutput));
        assertThat(withAttrs.pushedLimit(), equalTo(5));
        assertThat(withAttrs.mode(), equalTo(EqlRelation.Mode.SEQUENCE));
        assertTrue(withAttrs.hasExplicitLimit());
    }

    public void testEqualsIgnoresSourceAndPreResolvedFieldCaps() {
        // Two relations that differ only in Source and the coordinator-local field-caps carrier are equal: the carrier
        // is a resolution ES|QL happens to hold, not part of the plan's meaning.
        EqlRelation a = new EqlRelation(EMPTY, PATTERN, QUERY, OPTIONS, EqlRelation.Mode.EVENT, false, OUTPUT, 5, fieldCaps());
        EqlRelation b = new EqlRelation(
            new Source(1, 4, "EQL logs-* \"process where true\""),
            PATTERN,
            QUERY,
            OPTIONS,
            EqlRelation.Mode.EVENT,
            false,
            OUTPUT,
            5,
            null
        );
        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(
            relation(),
            r -> new EqlRelation(r.source(), PATTERN, r.query(), OPTIONS, r.mode(), r.hasExplicitLimit(), OUTPUT, r.pushedLimit(), null),
            EqlRelationTests::mutate
        );
    }

    private static EqlRelation mutate(EqlRelation r) {
        String query = r.query();
        EqlRelation.Mode mode = r.mode();
        boolean hasExplicitLimit = r.hasExplicitLimit();
        Integer pushedLimit = r.pushedLimit();
        switch (between(0, 3)) {
            case 0 -> query = query + " | head 1";
            case 1 -> mode = mode == EqlRelation.Mode.EVENT ? EqlRelation.Mode.SAMPLE : EqlRelation.Mode.EVENT;
            case 2 -> hasExplicitLimit = hasExplicitLimit == false;
            case 3 -> pushedLimit = 999;
            default -> throw new AssertionError();
        }
        return new EqlRelation(r.source(), PATTERN, query, OPTIONS, mode, hasExplicitLimit, OUTPUT, pushedLimit, null);
    }

    public void testToStringWrapsQueryAndSchema() {
        assertThat(relation().toString(), allOf(startsWith("EQL[" + QUERY + "]"), containsString("@timestamp")));
    }

    public void testWriteToThrowsCoordinatorLocal() {
        // Coordinator-local: the node never crosses the wire, so serialization is unsupported by contract.
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> relation().writeTo(null));
        assertThat(e.getMessage(), containsString("not serialized"));
    }

    public void testGetWriteableNameThrowsCoordinatorLocal() {
        expectThrows(UnsupportedOperationException.class, () -> relation().getWriteableName());
    }
}
