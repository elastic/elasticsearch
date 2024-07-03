/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ToPartialTests extends AbstractNodeTestCase<ToPartial, Expression> {
    @Override
    protected ToPartial randomInstance() {
        return randomToPartial();
    }

    @Override
    protected ToPartial mutate(ToPartial toPartial) {
        if (randomBoolean()) {
            var newFunction = randomValueOtherThanMany(
                newFn -> newFn == toPartial.function().getClass(),
                ToPartialTests::randomAggregateFunction
            );
            return new ToPartial(toPartial.source(), randomAggregateInstance(newFunction, toPartial.field()));
        } else {
            AggregateFunction function = (AggregateFunction) toPartial.function();
            Expression newArg = randomValueOtherThan(toPartial.field(), ToPartialTests::randomExpression);
            return new ToPartial(toPartial.source(), randomAggregateInstance(function.getClass(), newArg));
        }
    }

    @Override
    protected ToPartial copy(ToPartial toPartial) {
        final AggregateFunction fn = (AggregateFunction) toPartial.function();
        return new ToPartial(Source.EMPTY, fn);
    }

    @Override
    public void testTransform() {
        Expression inputArg = randomExpression();
        AggregateFunction function = randomAggregateInstance(randomAggregateFunction(), inputArg);
        ToPartial toPartial = new ToPartial(Source.EMPTY, function);
        {
            Expression newArg = randomValueOtherThan(toPartial.field(), ToPartialTests::randomExpression);
            ToPartial newToPartial = (ToPartial) toPartial.transformUp(Expression.class, e -> e.equals(inputArg) ? newArg : e);
            assertThat(newToPartial.function().getClass(), sameInstance(newToPartial.function().getClass()));
            assertThat(newToPartial.field(), equalTo(newArg));
        }
        {
            var newFunction = randomAggregateInstance(
                randomValueOtherThan(function.getClass(), ToPartialTests::randomAggregateFunction),
                inputArg
            );
            ToPartial newToPartial = (ToPartial) toPartial.transformUp(AggregateFunction.class, f -> f.equals(function) ? newFunction : f);
            assertThat(newToPartial.function(), sameInstance(newFunction));
            assertThat(newToPartial.field(), sameInstance(inputArg));
        }
    }

    @Override
    public void testReplaceChildren() {
        Expression inputArg = randomExpression();
        AggregateFunction function = randomAggregateInstance(randomAggregateFunction(), inputArg);
        ToPartial toPartial = new ToPartial(Source.EMPTY, function);
        Expression newArg = randomValueOtherThan(toPartial.field(), ToPartialTests::randomExpression);
        AggregateFunction newFunction = randomAggregateInstance(randomAggregateFunction(), newArg);
        ToPartial newToPartial = (ToPartial) toPartial.replaceChildren(List.of(newArg, newFunction));
        assertThat(newToPartial.function(), equalTo(newFunction));
        assertThat(newToPartial.field(), equalTo(newArg));
    }

    public static ToPartial randomToPartial() {
        try {
            AggregateFunction af = randomAggregateInstance(randomAggregateFunction(), randomExpression());
            ToPartial toPartial = new ToPartial(Source.EMPTY, af);
            assertSame(af.field(), toPartial.field());
            assertSame(toPartial.function(), af);
            return toPartial;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Expression randomExpression() {
        try {
            return EsqlNodeSubclassTests.makeNode(Expression.class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static Class<? extends AggregateFunction> randomAggregateFunction() {
        try {
            List<Class<? extends AggregateFunction>> functions = EsqlNodeSubclassTests.subclassesOf(AggregateFunction.class)
                .stream()
                .filter(clazz -> clazz != ToPartial.class && clazz != FromPartial.class)
                .toList();
            return randomFrom(functions);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static AggregateFunction randomAggregateInstance(Class<? extends AggregateFunction> af, Expression field) {
        try {
            var ctor = EsqlNodeSubclassTests.longestCtor(af);
            var args = EsqlNodeSubclassTests.ctorArgs(ctor);
            args[1] = field;
            return ctor.newInstance(args);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
