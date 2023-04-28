/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class InsertProcessorTests extends AbstractWireSerializingTestCase<InsertFunctionProcessor> {

    @Override
    protected InsertFunctionProcessor createTestInstance() {
        return new InsertFunctionProcessor(
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
            new ConstantProcessor(randomInt(256)),
            new ConstantProcessor(randomInt(128)),
            new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 256))
        );
    }

    @Override
    protected InsertFunctionProcessor mutateInstance(InsertFunctionProcessor instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<InsertFunctionProcessor> instanceReader() {
        return InsertFunctionProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testInsertWithValidInputs() {
        assertEquals("bazbar", new Insert(EMPTY, l("foobar"), l(1), l(3), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(4), l(3), l("baz")).makePipe().asProcessor().process(null));
    }

    public void testInsertWithEdgeCases() {
        assertNull(new Insert(EMPTY, l(null), l(4), l(3), l("baz")).makePipe().asProcessor().process(null));
        assertNull(new Insert(EMPTY, l("foobar"), l(null), l(3), l("baz")).makePipe().asProcessor().process(null));
        assertNull(new Insert(EMPTY, l("foobar"), l(4), l(null), l("baz")).makePipe().asProcessor().process(null));
        assertNull(new Insert(EMPTY, l("foobar"), l(4), l(3), l(null)).makePipe().asProcessor().process(null));
        assertEquals("bazbar", new Insert(EMPTY, l("foobar"), l(-1), l(3), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(4), l(30), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(6), l(1), l('z')).makePipe().asProcessor().process(null));
        assertEquals("foobarbaz", new Insert(EMPTY, l("foobar"), l(7), l(1000), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("foobar", new Insert(EMPTY, l("foobar"), l(8), l(1000), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("fzr", new Insert(EMPTY, l("foobar"), l(2), l(4), l('z')).makePipe().asProcessor().process(null));
        assertEquals("CAR", new Insert(EMPTY, l("FOOBAR"), l(1), l(5), l("CA")).makePipe().asProcessor().process(null));
        assertEquals("z", new Insert(EMPTY, l('f'), l(1), l(10), l('z')).makePipe().asProcessor().process(null));

        assertEquals("bla", new Insert(EMPTY, l(""), l(1), l(10), l("bla")).makePipe().asProcessor().process(null));
        assertEquals("", new Insert(EMPTY, l(""), l(2), l(10), l("bla")).makePipe().asProcessor().process(null));
    }

    public void testInsertInputsValidation() {
        SqlIllegalArgumentException siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l(5), l(1), l(3), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [5]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l(1), l(3), l(66)).makePipe().asProcessor().process(null)
        );
        assertEquals("A string/char is required; received [66]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l("c"), l(3), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A fixed point number is required for [start]; received [java.lang.String]", siae.getMessage());

        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l(1), l('z'), l("baz")).makePipe().asProcessor().process(null)
        );
        assertEquals("A fixed point number is required for [length]; received [java.lang.Character]", siae.getMessage());

        assertEquals(
            "baroobar",
            new Insert(EMPTY, l("foobar"), l(Integer.MIN_VALUE + 1), l(1), l("bar")).makePipe().asProcessor().process(null)
        );
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobarbar"), l(Integer.MIN_VALUE), l(1), l("bar")).makePipe().asProcessor().process(null)
        );
        assertEquals("[start] out of the allowed range [-2147483647, 2147483647], received [-2147483648]", siae.getMessage());

        assertEquals("foobar", new Insert(EMPTY, l("foobar"), l(Integer.MAX_VALUE), l(1), l("bar")).makePipe().asProcessor().process(null));
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l((long) Integer.MAX_VALUE + 1), l(1), l("bar")).makePipe().asProcessor().process(null)
        );
        assertEquals("[start] out of the allowed range [-2147483647, 2147483647], received [2147483648]", siae.getMessage());

        assertEquals("barfoobar", new Insert(EMPTY, l("foobar"), l(1), l(0), l("bar")).makePipe().asProcessor().process(null));
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l(1), l(-1), l("bar")).makePipe().asProcessor().process(null)
        );
        assertEquals("[length] out of the allowed range [0, 2147483647], received [-1]", siae.getMessage());

        assertEquals("bar", new Insert(EMPTY, l("foobar"), l(1), l(Integer.MAX_VALUE), l("bar")).makePipe().asProcessor().process(null));
        siae = expectThrows(
            SqlIllegalArgumentException.class,
            () -> new Insert(EMPTY, l("foobar"), l(1), l((long) Integer.MAX_VALUE + 1), l("bar")).makePipe().asProcessor().process(null)
        );
        assertEquals("[length] out of the allowed range [0, 2147483647], received [2147483648]", siae.getMessage());
    }
}
