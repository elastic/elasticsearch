/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.expression.function.scalar.whitelist.EqlAllocationEstimators;
import org.elasticsearch.xpack.ql.expression.Expression;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class ConcatFunctionProcessorTests extends ESTestCase {

    private static Object process(Object... arguments) {
        List<Expression> literals = new ArrayList<>(arguments.length);
        for (Object arg : arguments) {
            literals.add(l(arg));
        }
        return new Concat(EMPTY, literals).makePipe().asProcessor().process(null);
    }

    public void testConcat() {
        assertEquals(process(), "");
        assertNull(process((Object) null));
        assertEquals(process("foo"), "foo");
        assertEquals(process(true), "true");
        assertEquals(process(3.14), "3.14");
        assertEquals(process("foo", "::", "bar", "::", "baz"), "foo::bar::baz");
        assertNull(process("foo", "::", null, "::", "baz"));
        assertNull(process("foo", "::", null, "::", null));
        assertEquals(process("foo", "::", 1.0, "::", "baz"), "foo::1.0::baz");
    }

    public void testListConcatBytes() {
        // Text counts its real length, anything else a fixed 128 chars, on top of the 32 byte String overhead. The answer is the
        // same whether the list can be indexed or has to be iterated.
        List<Object> indexed = new ArrayList<>(List.of("foo", "::", 1.0, "baz"));
        List<Object> linked = new LinkedList<>(indexed);
        long expected = 32 + 2 * (3 + 2 + 128 + 3);
        assertEquals(expected, EqlAllocationEstimators.listConcatBytes(indexed));
        assertEquals(expected, EqlAllocationEstimators.listConcatBytes(linked));
        assertEquals(32, EqlAllocationEstimators.listConcatBytes(List.of()));
        assertEquals(32, EqlAllocationEstimators.listConcatBytes(null));
    }
}
