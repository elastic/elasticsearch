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
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;

import static org.elasticsearch.xpack.ql.expression.function.scalar.FunctionTestUtils.l;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class BinaryStringStringProcessorTests extends AbstractWireSerializingTestCase<BinaryStringStringProcessor> {

    @Override
    protected BinaryStringStringProcessor createTestInstance() {
        return new BinaryStringStringProcessor(
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(1, 128)),
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(1, 128)),
                randomFrom(BinaryStringStringOperation.values()));
    }

    @Override
    protected Reader<BinaryStringStringProcessor> instanceReader() {
        return BinaryStringStringProcessor::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    public void testPositionFunctionWithValidInput() {
        assertEquals(4, new Position(EMPTY, l("bar"), l("foobar")).makePipe().asProcessor().process(null));
        assertEquals(1, new Position(EMPTY, l("foo"), l("foobar")).makePipe().asProcessor().process(null));
        assertEquals(0, new Position(EMPTY, l("foo"), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(3, new Position(EMPTY, l('r'), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(0, new Position(EMPTY, l('z'), l("bar")).makePipe().asProcessor().process(null));
        assertEquals(1, new Position(EMPTY, l('b'), l('b')).makePipe().asProcessor().process(null));
    }

    public void testPositionFunctionWithEdgeCases() {
        assertNull(new Position(EMPTY, l("foo"), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Position(EMPTY, l(null), l("foo")).makePipe().asProcessor().process(null));
        assertNull(new Position(EMPTY, l(null), l(null)).makePipe().asProcessor().process(null));
    }

    public void testPositionFunctionInputsValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Position(EMPTY, l(5), l("foo")).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Position(EMPTY, l("foo bar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae.getMessage());
    }
}
