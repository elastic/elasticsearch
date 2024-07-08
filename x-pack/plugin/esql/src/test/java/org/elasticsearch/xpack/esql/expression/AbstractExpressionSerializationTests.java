/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTests;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.session.EsqlConfigurationSerializationTests;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public abstract class AbstractExpressionSerializationTests<T extends Expression> extends AbstractWireTestCase<T> {
    /**
     * We use a single random config for all serialization because it's pretty
     * heavy to build, especially in {@link #testConcurrentSerialization()}.
     */
    private EsqlConfiguration config;

    public static Source randomSource() {
        int lineNumber = between(0, EXAMPLE_QUERY.length - 1);
        int offset = between(0, EXAMPLE_QUERY[lineNumber].length() - 2);
        int length = between(1, EXAMPLE_QUERY[lineNumber].length() - offset - 1);
        String text = EXAMPLE_QUERY[lineNumber].substring(offset, offset + length);
        return new Source(lineNumber + 1, offset, text);
    }

    public static Expression randomChild() {
        return ReferenceAttributeTests.randomReferenceAttribute();
    }

    @Override
    protected final T copyInstance(T instance, TransportVersion version) throws IOException {
        return copyInstance(
            instance,
            getNamedWriteableRegistry(),
            (out, v) -> new PlanStreamOutput(out, new PlanNameRegistry(), config).writeNamedWriteable(v),
            in -> {
                PlanStreamInput pin = new PlanStreamInput(in, new PlanNameRegistry(), in.namedWriteableRegistry(), config);
                @SuppressWarnings("unchecked")
                T deser = (T) pin.readNamedWriteable(Expression.class);
                if (alwaysEmptySource()) {
                    assertThat(deser.source(), sameInstance(Source.EMPTY));
                } else {
                    assertThat(deser.source(), equalTo(instance.source()));
                }
                return deser;
            },
            version
        );
    }

    protected boolean alwaysEmptySource() {
        return false;
    }

    public EsqlConfiguration configuration() {
        return config;
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(NamedExpression.getNamedWriteables());
        entries.addAll(Expression.getNamedWriteables());
        entries.addAll(Attribute.getNamedWriteables());
        entries.addAll(EsqlScalarFunction.getNamedWriteables());
        entries.addAll(AggregateFunction.getNamedWriteables());
        entries.add(UnsupportedAttribute.ENTRY);
        entries.add(UnsupportedAttribute.NAMED_EXPRESSION_ENTRY);
        entries.add(UnsupportedAttribute.EXPRESSION_ENTRY);
        entries.addAll(EsField.getNamedWriteables());
        entries.add(org.elasticsearch.xpack.esql.expression.Order.ENTRY);
        return new NamedWriteableRegistry(entries);
    }

    private static final String[] EXAMPLE_QUERY = new String[] {
        "I am the very model of a modern Major-Gineral,",
        "I've information vegetable, animal, and mineral,",
        "I know the kings of England, and I quote the fights historical",
        "From Marathon to Waterloo, in order categorical;",
        "I'm very well acquainted, too, with matters mathematical,",
        "I understand equations, both the simple and quadratical,",
        "About binomial theorem I'm teeming with a lot o' news,",
        "With many cheerful facts about the square of the hypotenuse." };

    @Before
    public void initConfig() {
        config = EsqlConfigurationSerializationTests.randomConfiguration(String.join("\n", EXAMPLE_QUERY), Map.of());
    }
}
