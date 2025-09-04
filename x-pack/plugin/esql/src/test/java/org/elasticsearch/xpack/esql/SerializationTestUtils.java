/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

public class SerializationTestUtils {
    public static void assertSerialization(PhysicalPlan plan) {
        assertSerialization(plan, EsqlTestUtils.TEST_CFG);
    }

    public static void assertSerialization(PhysicalPlan plan, Configuration configuration) {
        var deserPlan = serializeDeserialize(
            plan,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(PhysicalPlan.class),
            configuration
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(plan, unused -> deserPlan);
    }

    public static void assertSerialization(LogicalPlan plan) {
        var deserPlan = serializeDeserialize(plan, PlanStreamOutput::writeNamedWriteable, in -> in.readNamedWriteable(LogicalPlan.class));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(plan, unused -> deserPlan);
    }

    public static void assertSerialization(Expression expression) {
        assertSerialization(expression, EsqlTestUtils.TEST_CFG);
    }

    public static void assertSerialization(Expression expression, Configuration configuration) {
        Expression deserExpression = serializeDeserialize(
            expression,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(Expression.class),
            configuration
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(expression, unused -> deserExpression);
    }

    public static <T> T serializeDeserialize(T orig, Serializer<T> serializer, Deserializer<T> deserializer) {
        return serializeDeserialize(orig, serializer, deserializer, EsqlTestUtils.TEST_CFG);
    }

    public static <T> T serializeDeserialize(T orig, Serializer<T> serializer, Deserializer<T> deserializer, Configuration config) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            PlanStreamOutput planStreamOutput = new PlanStreamOutput(out, config);
            serializer.write(planStreamOutput, orig);
            StreamInput in = new NamedWriteableAwareStreamInput(
                ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())),
                writableRegistry()
            );
            PlanStreamInput planStreamInput = new PlanStreamInput(in, in.namedWriteableRegistry(), config);
            return deserializer.read(planStreamInput);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public interface Serializer<T> {
        void write(PlanStreamOutput out, T object) throws IOException;
    }

    public interface Deserializer<T> {
        T read(PlanStreamInput in) throws IOException;
    }

    public static NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, TermsQueryBuilder.NAME, TermsQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, RangeQueryBuilder.NAME, RangeQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, RegexpQueryBuilder.NAME, RegexpQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, ExistsQueryBuilder.NAME, ExistsQueryBuilder::new));
        entries.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, KnnVectorQueryBuilder.NAME, KnnVectorQueryBuilder::new));
        entries.add(SingleValueQuery.ENTRY);
        entries.addAll(ExpressionWritables.getNamedWriteables());
        entries.addAll(PlanWritables.getNamedWriteables());
        entries.add(
            new NamedWriteableRegistry.Entry(
                GenericNamedWriteable.class,
                AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral.ENTRY.name,
                AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral::new
            )
        );
        return new NamedWriteableRegistry(entries);
    }
}
