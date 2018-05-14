/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

import java.io.IOException;

public class StringFunctionProcessorTests extends AbstractWireSerializingTestCase<StringProcessor> {
    public static StringProcessor randomStringFunctionProcessor() {
        return new StringProcessor(randomFrom(StringOperation.values()));
    }

    @Override
    protected StringProcessor createTestInstance() {
        return randomStringFunctionProcessor();
    }

    @Override
    protected Reader<StringProcessor> instanceReader() {
        return StringProcessor::new;
    }

    @Override
    protected StringProcessor mutateInstance(StringProcessor instance) throws IOException {
        return new StringProcessor(randomValueOtherThan(instance.processor(), () -> randomFrom(StringOperation.values())));
    }

    public void testAscii() {
        StringProcessor proc = new StringProcessor(StringOperation.ASCII);
        assertNull(proc.process(null));
        assertEquals(65, proc.process("A"));
        // accepts chars as well
        assertEquals(65, proc.process('A'));
        assertEquals(65, proc.process("Alpha"));
        // validate input

    }

    public void testAsciiInputCheck() {
        StringProcessor proc = new StringProcessor(StringOperation.ASCII);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process(12));
        assertEquals("A string/char is required; received [12]", siae.getMessage());
    }

    public void testChar() {
        StringProcessor proc = new StringProcessor(StringOperation.CHAR);
        assertNull(proc.process(null));
        assertEquals('A', proc.process(65));
        assertNull(proc.process(256));
        assertNull(proc.process(-1));
        // validate input
        expectThrows(SqlIllegalArgumentException.class, () -> proc.process("A"));
    }

    public void testCharInputCheck() {
        StringProcessor proc = new StringProcessor(StringOperation.CHAR);
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("A"));
        assertEquals("A number is required; received [A]", siae.getMessage());
    }
}
