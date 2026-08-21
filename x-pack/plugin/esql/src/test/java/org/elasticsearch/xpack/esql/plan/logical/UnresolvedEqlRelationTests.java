/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.IndexPattern;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for the parser-produced {@link UnresolvedEqlRelation} leaf: it reports itself unresolved (so the analyzer's
 * {@code ResolveEqlRelation} must replace it), exposes an empty output until then, labels itself {@code EQL} for
 * telemetry, and — being coordinator-local like its resolved counterpart — throws on serialization.
 */
public class UnresolvedEqlRelationTests extends ESTestCase {

    private static final IndexPattern PATTERN = new IndexPattern(EMPTY, "logs-*");
    private static final Expression QUERY = Literal.keyword(EMPTY, "process where true");
    private static final Map<String, Object> OPTIONS = Map.of("size", 10);
    private static final List<NamedExpression> METADATA = List.of(MetadataAttribute.create(EMPTY, "_id"));

    private static UnresolvedEqlRelation relation() {
        return new UnresolvedEqlRelation(EMPTY, PATTERN, QUERY, OPTIONS, METADATA);
    }

    public void testUnresolvedContract() {
        UnresolvedEqlRelation relation = relation();
        assertFalse("the parser node is unresolved by construction", relation.resolved());
        assertFalse(relation.expressionsResolved());
        assertThat("no schema until the analyzer resolves it", relation.output(), empty());
        assertThat(relation.unresolvedMessage(), containsString("Unresolved EQL query"));
    }

    public void testAccessors() {
        UnresolvedEqlRelation relation = relation();
        assertThat(relation.indexPattern(), equalTo(PATTERN));
        assertThat(relation.query(), equalTo(QUERY));
        assertThat(relation.options(), equalTo(OPTIONS));
        assertThat(relation.metadataFields().stream().map(NamedExpression::name).toList(), contains("_id"));
    }

    public void testTelemetryLabelIsEql() {
        // Telemetry walks the pre-analysis plan, so this node — not EqlRelation — carries the command's label.
        assertThat(relation().telemetryLabel(), equalTo("EQL"));
    }

    public void testNodePropertiesExposeQuery() {
        assertThat(relation().nodeProperties(), contains(QUERY));
    }

    public void testToStringHasUnresolvedPrefix() {
        assertThat(relation().toString(), startsWith("?EQL["));
    }

    public void testCustomUnresolvedMessageIsCarried() {
        UnresolvedEqlRelation relation = new UnresolvedEqlRelation(EMPTY, PATTERN, QUERY, OPTIONS, METADATA, "Unknown index [logs-*]");
        assertThat(relation.unresolvedMessage(), equalTo("Unknown index [logs-*]"));
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(
            relation(),
            r -> new UnresolvedEqlRelation(r.source(), PATTERN, QUERY, OPTIONS, METADATA),
            r -> new UnresolvedEqlRelation(r.source(), PATTERN, Literal.keyword(EMPTY, "network where true"), OPTIONS, METADATA)
        );
    }

    public void testWriteToThrowsCoordinatorLocal() {
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> relation().writeTo(null));
        assertThat(e.getMessage(), containsString("not serialized"));
    }

    public void testGetWriteableNameThrowsCoordinatorLocal() {
        expectThrows(UnsupportedOperationException.class, () -> relation().getWriteableName());
    }
}
