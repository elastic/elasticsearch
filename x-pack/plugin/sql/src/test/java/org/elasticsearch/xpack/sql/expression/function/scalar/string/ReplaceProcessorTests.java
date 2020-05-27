/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

public class ReplaceProcessorTests extends AbstractWireSerializingTestCase<ReplaceFunctionProcessor> {
    
    @Override
    protected ReplaceFunctionProcessor createTestInstance() {
        return new ReplaceFunctionProcessor(
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)), 
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)));
    }

    @Override
    protected Reader<ReplaceFunctionProcessor> instanceReader() {
        return ReplaceFunctionProcessor::new;
    }
    
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }
        
    public void testReplaceFunctionWithValidInput() {
        assertEquals("foobazbaz",
                new Replace(EMPTY, l("foobarbar"), l("bar"), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("foobXrbXr", new Replace(EMPTY, l("foobarbar"), l('a'), l('X')).makePipe().asProcessor().process(null));
        assertEquals("z", new Replace(EMPTY, l('f'), l('f'), l('z')).makePipe().asProcessor().process(null));
    }
    
    public void testReplaceFunctionWithEdgeCases() {
        assertEquals("foobarbar",
                new Replace(EMPTY, l("foobarbar"), l("bar"), l(null)).makePipe().asProcessor().process(null));
        assertEquals("foobarbar",
                new Replace(EMPTY, l("foobarbar"), l(null), l("baz")).makePipe().asProcessor().process(null));
        assertNull(new Replace(EMPTY, l(null), l("bar"), l("baz")).makePipe().asProcessor().process(null));
        assertNull(new Replace(EMPTY, l(null), l(null), l(null)).makePipe().asProcessor().process(null));
    }
    
    public void testReplaceFunctionInputsValidation() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Replace(EMPTY, l(5), l("bar"), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Replace(EMPTY, l("foobarbar"), l(4), l("baz")).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [4]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Replace(EMPTY, l("foobarbar"), l("bar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae.getMessage());
    }
}
