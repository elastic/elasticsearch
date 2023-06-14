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
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public class SerializationTestUtils {

    private static final PlanNameRegistry planNameRegistry = new PlanNameRegistry();

    public static void assertSerialization(PhysicalPlan plan) {
        var deserPlan = serializeDeserialize(plan, PlanStreamOutput::writePhysicalPlanNode, PlanStreamInput::readPhysicalPlanNode);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(plan, unused -> deserPlan);
    }

    public static void assertSerialization(Expression expression) {
        Expression deserExpression = serializeDeserialize(expression, PlanStreamOutput::writeExpression, PlanStreamInput::readExpression);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(expression, unused -> deserExpression);
    }

    public static <T> T serializeDeserialize(T orig, Serializer<T> serializer, Deserializer<T> deserializer) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            PlanStreamOutput planStreamOutput = new PlanStreamOutput(out, planNameRegistry);
            serializer.write(planStreamOutput, orig);
            StreamInput in = new NamedWriteableAwareStreamInput(
                ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())),
                writableRegistry()
            );
            PlanStreamInput planStreamInput = new PlanStreamInput(in, planNameRegistry, writableRegistry());
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
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermsQueryBuilder.NAME, TermsQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, RangeQueryBuilder.NAME, RangeQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, BoolQueryBuilder.NAME, BoolQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, WildcardQueryBuilder.NAME, WildcardQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, RegexpQueryBuilder.NAME, RegexpQueryBuilder::new),
                SingleValueQuery.ENTRY
            )
        );
    }
}
