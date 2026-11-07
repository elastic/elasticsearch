/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.List;

/**
 * {@code TYPED} carries a second expression that is not one of its children, so the node's own {@code writeTo} writes
 * it and nothing about the children would notice if it stopped. What the literal is <em>for</em> is the node's type, so
 * these read it back through {@code dataType()} rather than through an accessor added for the purpose.
 */
public class TypedSerializationTests extends AbstractExpressionSerializationTests<Typed> {

    /**
     * Type names that survive the round trip <em>through {@link Typed#dataType()}</em>, which is how
     * {@link #mutateInstance} reads back the type an instance was built with. That rules out {@code text}: it is
     * spelled as its own type but {@code dataType()} narrows it to {@code keyword}, so a mutation away from "keyword"
     * could land on "text" and produce a node equal to the original - a one-in-144 draw over twenty runs, which is a
     * flake rather than a failure. {@code TypedTests} covers a text target in full.
     */
    private static final List<String> ROUND_TRIPPING = List.of(
        "boolean",
        "double",
        "integer",
        "ip",
        "keyword",
        "long",
        "unsigned_long",
        "version"
    );

    /** As the parser builds it: a string literal folds to a {@link org.apache.lucene.util.BytesRef}, not to a String. */
    private static Literal typeName(String name) {
        return new Literal(randomSource(), BytesRefs.toBytesRef(name), DataType.KEYWORD);
    }

    @Override
    protected Typed createTestInstance() {
        return new Typed(randomSource(), randomChild(), typeName(randomFrom(ROUND_TRIPPING)));
    }

    @Override
    protected Typed mutateInstance(Typed instance) throws IOException {
        String type = instance.dataType().esType();
        Expression field = instance.field();
        if (randomBoolean()) {
            field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
        } else {
            type = randomValueOtherThan(type, () -> randomFrom(ROUND_TRIPPING));
        }
        return new Typed(instance.source(), field, typeName(type));
    }

    /**
     * The literal decides the node's type, so losing it on the wire would land a node that types itself
     * {@code UNSUPPORTED} - the value {@code dataType()} returns when the target cannot be read.
     */
    public void testTheTypeTheLiteralNamesSurvivesTheRoundTrip() throws IOException {
        Typed typed = new Typed(randomSource(), randomChild(), typeName("ip"));
        assertEquals(DataType.IP, typed.dataType());
        assertEquals(DataType.IP, copyInstance(typed).dataType());
    }

    /**
     * Two nodes over one field asking for different types are different nodes. Were the literal dropped they would
     * arrive equal, and anything keyed on the node would serve one query's answer to the other.
     */
    public void testTwoTypesOverOneFieldAreNotEqual() {
        Expression field = randomChild();
        assertNotEquals(new Typed(randomSource(), field, typeName("long")), new Typed(randomSource(), field, typeName("keyword")));
    }
}
