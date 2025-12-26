/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AvgSerializationTests extends AbstractExpressionSerializationTests<Avg> {
    @Override
    protected Avg createTestInstance() {
        return new Avg(randomSource(), randomChild(), randomChild(), randomChild(), randomChild());
    }

    @Override
    protected Avg mutateInstance(Avg instance) throws IOException {
        Expression field = instance.field();
        Expression filter = instance.filter();
        Expression window = instance.window();
        Expression summationMode = instance.summationMode();
        switch (randomIntBetween(0, 3)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> filter = randomValueOtherThan(filter, AbstractExpressionSerializationTests::randomChild);
            case 2 -> window = randomValueOtherThan(window, AbstractExpressionSerializationTests::randomChild);
            case 3 -> summationMode = randomValueOtherThan(summationMode, AbstractExpressionSerializationTests::randomChild);
            default -> throw new AssertionError("unexpected value");
        }
        return new Avg(instance.source(), field, filter, window, summationMode);
    }

    public static class OldAvg extends AggregateFunction {
        public OldAvg(Source source, Expression field, Expression filter) {
            super(source, field, filter, NO_WINDOW, List.of());
        }

        @Override
        public AggregateFunction withFilter(Expression filter) {
            return new OldAvg(source(), filter, filter);
        }

        @Override
        public DataType dataType() {
            return field().dataType();
        }

        @Override
        public Expression replaceChildren(List<Expression> newChildren) {
            return new OldAvg(source(), newChildren.get(0), newChildren.get(1));
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, OldAvg::new, field(), filter());
        }

        @Override
        public String getWriteableName() {
            return Avg.ENTRY.name;
        }
    }

    public void testSerializeOldAvg() throws IOException {
        var oldAvg = new OldAvg(randomSource(), randomChild(), randomChild());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            PlanStreamOutput planOut = new PlanStreamOutput(out, configuration());
            planOut.writeNamedWriteable(oldAvg);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), getNamedWriteableRegistry())) {
                PlanStreamInput planIn = new PlanStreamInput(
                    in,
                    getNamedWriteableRegistry(),
                    configuration(),
                    new SerializationTestUtils.TestNameIdMapper()
                );
                Avg serialized = (Avg) planIn.readNamedWriteable(categoryClass());
                assertThat(serialized.source(), equalTo(oldAvg.source()));
                assertThat(serialized.field(), equalTo(oldAvg.field()));
                assertThat(serialized.filter(), equalTo(oldAvg.filter()));
                assertThat(serialized.summationMode(), equalTo(SummationMode.COMPENSATED_LITERAL));
            }
        }
    }
}
