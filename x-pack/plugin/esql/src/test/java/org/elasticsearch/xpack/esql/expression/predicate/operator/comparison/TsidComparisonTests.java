/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesHash;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.BitSet;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Binary identifiers support equality without becoming strings or gaining an ordering. */
public class TsidComparisonTests extends ESTestCase {
    private static final TransportVersion VERSION = TransportVersion.fromName("promql_functional_timeseries");

    public void testBinarySlicesAndNulls() {
        byte[] storage = randomByteArrayOfLength(24);
        storage[4] = (byte) 0xff;
        storage[5] = (byte) 0x80;
        BytesRef slice = new BytesRef(storage, 4, 16);
        Literal value = tsid(slice);
        Literal equal = tsid(BytesRef.deepCopyOf(slice));
        BytesRef different = BytesRef.deepCopyOf(slice);
        different.bytes[15] ^= 1;
        Literal other = tsid(different);
        assertEquals(true, new Equals(Source.EMPTY, value, equal).fold(FoldContext.small()));
        assertEquals(false, new Equals(Source.EMPTY, value, other).fold(FoldContext.small()));
        assertEquals(false, new NotEquals(Source.EMPTY, value, equal).fold(FoldContext.small()));
        assertEquals(true, new NotEquals(Source.EMPTY, value, other).fold(FoldContext.small()));
        assertNull(new Equals(Source.EMPTY, value, Literal.NULL).fold(FoldContext.small()));
        assertNull(new NotEquals(Source.EMPTY, Literal.NULL, value).fold(FoldContext.small()));
        assertEquals(true, new In(Source.EMPTY, value, List.of(other, Literal.NULL, equal)).fold(FoldContext.small()));
        assertEquals(false, new In(Source.EMPTY, value, List.of(other)).fold(FoldContext.small()));
        assertNull(new In(Source.EMPTY, value, List.of(other, Literal.NULL)).fold(FoldContext.small()));
        assertNull(new In(Source.EMPTY, Literal.NULL, List.of(value)).fold(FoldContext.small()));
        assertEquals(true, new Not(Source.EMPTY, new In(Source.EMPTY, value, List.of(other))).fold(FoldContext.small()));
    }

    public void testHashResultsCanBeCompared() {
        var first = hash("a");
        var same = hash("a");
        var other = hash("b");
        assertEquals(true, new Equals(Source.EMPTY, first, same).fold(FoldContext.small()));
        assertEquals(true, new NotEquals(Source.EMPTY, first, other).fold(FoldContext.small()));
        assertEquals(true, new In(Source.EMPTY, first, List.of(other, same)).fold(FoldContext.small()));
    }

    public void testNoStringCoercionOrOrdering() {
        Literal value = tsid(new BytesRef(randomByteArrayOfLength(16)));
        Literal string = Literal.keyword(Source.EMPTY, "value");
        assertTrue(new Equals(Source.EMPTY, value, string).typeResolved().unresolved());
        assertTrue(new NotEquals(Source.EMPTY, value, string).typeResolved().unresolved());
        assertTrue(new In(Source.EMPTY, value, List.of(string)).typeResolved().unresolved());
        assertTrue(new GreaterThan(Source.EMPTY, value, value).typeResolved().unresolved());
        assertTrue(new LessThan(Source.EMPTY, value, value).typeResolved().unresolved());
    }

    public void testCurrentNodesSupportStoredTsidComparisons() {
        for (String query : queries()) {
            analyzer().addK8s().minimumTransportVersion(VERSION).query(query);
        }
    }

    public void testOlderNodesRejectStoredTsidComparisons() {
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(VERSION);
        for (String query : queries()) {
            var error = expectThrows(VerificationException.class, () -> analyzer().addK8s().minimumTransportVersion(previous).query(query));
            assertThat(
                query,
                error.getMessage(),
                containsString("Comparisons of [_tsid] values require support on every participating node")
            );
        }
        analyzer().addK8s().minimumTransportVersion(previous).query("FROM k8s METADATA _tsid | KEEP _tsid | LIMIT 10");
    }

    public void testInnerJoinTsidKeysRequireCurrentNodes() {
        var leftKey = new ReferenceAttribute(Source.EMPTY, "left_key", DataType.TSID_DATA_TYPE);
        var rightKey = new ReferenceAttribute(Source.EMPTY, "right_key", DataType.TSID_DATA_TYPE);
        var join = new InnerJoin(
            Source.EMPTY,
            new LocalRelation(Source.EMPTY, List.of(leftKey), EmptyLocalSupplier.EMPTY),
            new LocalRelation(Source.EMPTY, List.of(rightKey), EmptyLocalSupplier.EMPTY),
            List.of(leftKey),
            List.of(rightKey),
            List.of(),
            true
        );
        analyzer().minimumTransportVersion(VERSION).buildAnalyzer().verify(join, new BitSet());
        var error = expectThrows(
            VerificationException.class,
            () -> analyzer().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(VERSION))
                .buildAnalyzer()
                .verify(join, new BitSet())
        );
        assertThat(error.getMessage(), containsString("Comparisons of [_tsid] values require support on every participating node"));
    }

    /** Inline aggregation joins preserve binary fingerprints without treating them as strings. */
    public void testInlineJoinTsidKeysRequireCurrentNodes() {
        var step = new ReferenceAttribute(Source.EMPTY, "step", DataType.DATETIME);
        var key = new ReferenceAttribute(Source.EMPTY, "key", DataType.TSID_DATA_TYPE);
        var priority = new ReferenceAttribute(Source.EMPTY, "priority", DataType.INTEGER);
        var source = new LocalRelation(Source.EMPTY, List.of(step, key, priority), EmptyLocalSupplier.EMPTY);
        var minimum = new Alias(Source.EMPTY, "minimum", new Min(Source.EMPTY, priority));
        var aggregate = new Aggregate(Source.EMPTY, source, List.of(step, key), List.of(minimum, step, key));
        var join = (InlineJoin) new InlineStats(Source.EMPTY, aggregate).surrogate();
        assertEquals(List.of(step, key), join.config().leftFields());
        assertEquals(DataType.TSID_DATA_TYPE, join.config().rightFields().get(1).dataType());
        analyzer().minimumTransportVersion(VERSION).buildAnalyzer().verify(join, new BitSet());
        var error = expectThrows(
            VerificationException.class,
            () -> analyzer().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(VERSION))
                .buildAnalyzer()
                .verify(join, new BitSet())
        );
        assertThat(error.getMessage(), containsString("Comparisons of [_tsid] values require support on every participating node"));
    }

    private static List<String> queries() {
        return List.of(
            "FROM k8s METADATA _tsid | EVAL matched = _tsid == _tsid | KEEP matched | LIMIT 10",
            "FROM k8s METADATA _tsid | EVAL matched = _tsid != _tsid | KEEP matched | LIMIT 10",
            "FROM k8s METADATA _tsid | EVAL matched = _tsid IN (_tsid) | KEEP matched | LIMIT 10",
            "FROM k8s METADATA _tsid | EVAL matched = _tsid NOT IN (_tsid) | KEEP matched | LIMIT 10",
            "FROM k8s METADATA _tsid | WHERE _tsid IN (FROM k8s METADATA _tsid | KEEP _tsid) | KEEP _tsid | LIMIT 10",
            "FROM k8s METADATA _tsid | WHERE _tsid NOT IN (FROM k8s METADATA _tsid | KEEP _tsid) | KEEP _tsid | LIMIT 10"
        );
    }

    private static Literal tsid(BytesRef value) {
        return new Literal(Source.EMPTY, value, DataType.TSID_DATA_TYPE);
    }

    private static TimeSeriesHash hash(String host) {
        return new TimeSeriesHash(
            Source.EMPTY,
            Literal.NULL,
            List.of(Literal.keyword(Source.EMPTY, "host"), Literal.keyword(Source.EMPTY, host))
        );
    }
}
