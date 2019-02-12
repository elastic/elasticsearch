/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.gen.processor.ConstantProcessor;

import static org.elasticsearch.xpack.sql.tree.Source.EMPTY;
import static org.elasticsearch.xpack.sql.expression.function.scalar.FunctionTestUtils.l;

public class LocateProcessorTests extends AbstractWireSerializingTestCase<LocateFunctionProcessor> {
    
    @Override
    protected LocateFunctionProcessor createTestInstance() {
        // the "start" parameter is optional and is treated as null in the constructor
        // when it is not used. Need to take this into account when generating random
        // values for it.
        Integer start = frequently() ? randomInt() : null;
        return new LocateFunctionProcessor(
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)), 
                new ConstantProcessor(randomRealisticUnicodeOfLengthBetween(0, 128)),
                new ConstantProcessor(start));
    }

    @Override
    protected Reader<LocateFunctionProcessor> instanceReader() {
        return LocateFunctionProcessor::new;
    }
    
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }
    
    public void testLocateFunctionWithValidInput() {
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(7, new Locate(EMPTY, l("bar"), l("foobarbar"), l(5)).makePipe().asProcessor().process(null));
    }
    
    public void testLocateFunctionWithEdgeCasesInputs() {
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar"), l(null)).makePipe().asProcessor().process(null));
        assertNull(new Locate(EMPTY, l("bar"), l(null), l(3)).makePipe().asProcessor().process(null));
        assertEquals(0, new Locate(EMPTY, l(null), l("foobarbar"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(0, new Locate(EMPTY, l(null), l("foobarbar"), l(null)).makePipe().asProcessor().process(null));

        assertEquals(1, new Locate(EMPTY, l("foo"), l("foobarbar"), l(null)).makePipe().asProcessor().process(null));
        assertEquals(1, new Locate(EMPTY, l('o'), l('o'), l(null)).makePipe().asProcessor().process(null));
        assertEquals(9, new Locate(EMPTY, l('r'), l("foobarbar"), l(9)).makePipe().asProcessor().process(null));
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar"), l(-3)).makePipe().asProcessor().process(null));
    }
    
    public void testLocateFunctionValidatingInputs() {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Locate(EMPTY, l(5), l("foobarbar"), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Locate(EMPTY, l("foo"), l(1), l(3)).makePipe().asProcessor().process(null));
        assertEquals("A string/char is required; received [1]", siae.getMessage());
        siae = expectThrows(SqlIllegalArgumentException.class,
                () -> new Locate(EMPTY, l("foobarbar"), l("bar"), l('c')).makePipe().asProcessor().process(null));
        assertEquals("A number is required; received [c]", siae.getMessage());
    }
}
