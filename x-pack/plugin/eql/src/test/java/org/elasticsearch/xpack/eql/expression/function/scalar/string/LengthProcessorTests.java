/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.LiteralTests;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

public class LengthProcessorTests extends ESTestCase {

    public void testLengthFunctionWithValidInput() {
        assertEquals(9, new Length(EMPTY, l("foobarbar")).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l("")).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l(12)).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l(true)).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l(0.1f)).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l("", IP)).makePipe().asProcessor().process(null));
        assertEquals(0, new Length(EMPTY, l("", DATETIME)).makePipe().asProcessor().process(null));
    }
    
    public void testLengthFunctionWithRandomDataType() {
        Literal literal = randomValueOtherThanMany(v -> v.dataType() == KEYWORD, () -> LiteralTests.randomLiteral());
        assertEquals(0, new Length(EMPTY, literal).makePipe().asProcessor().process(null));
    }
}
