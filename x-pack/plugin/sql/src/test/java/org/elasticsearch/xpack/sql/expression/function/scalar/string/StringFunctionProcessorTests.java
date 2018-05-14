/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

import java.io.IOException;

import static org.elasticsearch.xpack.sql.tree.Location.EMPTY;

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
    
    private void stringCharInputValidation(StringProcessor proc) {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process(123));
        assertEquals("A string/char is required; received [123]", siae.getMessage());
    }
    
    private void numericInputValidation(StringProcessor proc) {
        SqlIllegalArgumentException siae = expectThrows(SqlIllegalArgumentException.class, () -> proc.process("A"));
        assertEquals("A number is required; received [A]", siae.getMessage());
    }

    public void testAscii() {
        StringProcessor proc = new StringProcessor(StringOperation.ASCII);
        assertNull(proc.process(null));
        assertEquals(65, proc.process("A"));
        // accepts chars as well
        assertEquals(65, proc.process('A'));
        assertEquals(65, proc.process("Alpha"));
        // validate input
        stringCharInputValidation(proc);
    }

    public void testChar() {
        StringProcessor proc = new StringProcessor(StringOperation.CHAR);
        assertNull(proc.process(null));
        assertEquals("A", proc.process(65));
        assertNull(proc.process(256));
        assertNull(proc.process(-1));
        // validate input
        numericInputValidation(proc);
    }
    
    public void testLCase() {
        StringProcessor proc = new StringProcessor(StringOperation.LCASE);
        assertNull(proc.process(null));
        assertEquals("fulluppercase", proc.process("FULLUPPERCASE"));
        assertEquals("someuppercase", proc.process("SomeUpPerCasE"));
        assertEquals("fulllowercase", proc.process("fulllowercase"));
        assertEquals("a", proc.process('A'));
        
        stringCharInputValidation(proc);
    }
    
    public void testUCase() {
        StringProcessor proc = new StringProcessor(StringOperation.UCASE);
        assertNull(proc.process(null));
        assertEquals("FULLLOWERCASE", proc.process("fulllowercase"));
        assertEquals("SOMELOWERCASE", proc.process("SomeLoweRCasE"));
        assertEquals("FULLUPPERCASE", proc.process("FULLUPPERCASE"));
        assertEquals("A", proc.process('a'));
        
        stringCharInputValidation(proc);
    }
    
    public void testLength() {
        StringProcessor proc = new StringProcessor(StringOperation.LENGTH);
        assertNull(proc.process(null));
        assertEquals(7, proc.process("foo bar"));
        assertEquals(0, proc.process(""));
        assertEquals(0, proc.process("    "));
        assertEquals(7, proc.process("foo bar   "));
        assertEquals(10, proc.process("   foo bar   "));
        assertEquals(1, proc.process('f'));
        
        stringCharInputValidation(proc);
    }
    
    public void testRTrim() {
        StringProcessor proc = new StringProcessor(StringOperation.RTRIM);
        assertNull(proc.process(null));
        assertEquals("foo bar", proc.process("foo bar"));
        assertEquals("", proc.process(""));
        assertEquals("", proc.process("    "));
        assertEquals("foo bar", proc.process("foo bar   "));
        assertEquals("   foo bar", proc.process("   foo bar   "));
        assertEquals("f", proc.process('f'));
        
        stringCharInputValidation(proc);
    }
    
    public void testLTrim() {
        StringProcessor proc = new StringProcessor(StringOperation.LTRIM);
        assertNull(proc.process(null));
        assertEquals("foo bar", proc.process("foo bar"));
        assertEquals("", proc.process(""));
        assertEquals("", proc.process("    "));
        assertEquals("foo bar", proc.process("   foo bar"));
        assertEquals("foo bar   ", proc.process("   foo bar   "));
        assertEquals("f", proc.process('f'));
        
        stringCharInputValidation(proc);
    }
    
    public void testSpace() {
        StringProcessor proc = new StringProcessor(StringOperation.SPACE);
        int count = 7;
        assertNull(proc.process(null));
        assertEquals("       ", proc.process(count));
        assertEquals(count, ((String) proc.process(count)).length());
        assertNotNull(proc.process(0));
        assertEquals("", proc.process(0));
        assertNull(proc.process(-1));

        numericInputValidation(proc);
    }
    
    public void testBitLength() {
        StringProcessor proc = new StringProcessor(StringOperation.BIT_LENGTH);
        assertNull(proc.process(null));
        assertEquals(56, proc.process("foo bar"));
        assertEquals(0, proc.process(""));
        assertEquals(8, proc.process('f'));
        
        stringCharInputValidation(proc);
    }
    
    public void testCharLength() {
        StringProcessor proc = new StringProcessor(StringOperation.CHAR_LENGTH);
        assertNull(proc.process(null));
        assertEquals(7, proc.process("foo bar"));
        assertEquals(0, proc.process(""));
        assertEquals(1, proc.process('f'));
        assertEquals(1, proc.process('€'));
        
        stringCharInputValidation(proc);
    }
    
    public void testLeft() {
        // test the operation on a string/char
        assertEquals("foo", new Left(EMPTY, l("foo bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo bar", new Left(EMPTY, l("foo bar"), l(7)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo bar", new Left(EMPTY, l("foo bar"), l(123)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("f", new Left(EMPTY, l('f'), l(1)).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge cases situations
        assertNull(new Left(EMPTY, l("foo bar"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Left(EMPTY, l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Left(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l("foo bar"), l(-1)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l("foo bar"), l(0)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Left(EMPTY, l('f'), l(0)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Left(EMPTY, l(5), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Left(EMPTY, l("foo bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [baz]", siae2.getMessage());
    }
    
    public void testRight() {
        // test the operation on a string/char
        assertEquals("bar", new Right(EMPTY, l("foo bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo bar", new Right(EMPTY, l("foo bar"), l(7)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo bar", new Right(EMPTY, l("foo bar"), l(123)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("f", new Right(EMPTY, l('f'), l(1)).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertNull(new Right(EMPTY, l("foo bar"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Right(EMPTY, l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Right(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l("foo bar"), l(-1)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l("foo bar"), l(0)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Right(EMPTY, l('f'), l(0)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Right(EMPTY, l(5), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Right(EMPTY, l("foo bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [baz]", siae2.getMessage());
    }
    
    public void testRepeat() {
        // test the operation on a string/char
        assertEquals("foofoofoo", new Repeat(EMPTY, l("foo"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo", new Repeat(EMPTY, l("foo"), l(1)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("fff", new Repeat(EMPTY, l('f'), l(3)).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertNull(new Repeat(EMPTY, l("foo"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l("foo"), l(-1)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Repeat(EMPTY, l("foo"), l(0)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Repeat(EMPTY, l(5), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Repeat(EMPTY, l("foo bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [baz]", siae2.getMessage());
    }
    
    public void testPosition() {
        // test the operation on a string/char
        assertEquals(4, new Position(EMPTY, l("bar"), l("foobar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(1, new Position(EMPTY, l("foo"), l("foobar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(0, new Position(EMPTY, l("foo"), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(3, new Position(EMPTY, l('r'), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(0, new Position(EMPTY, l('z'), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(1, new Position(EMPTY, l('b'), l('b')).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertNull(new Position(EMPTY, l("foo"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Position(EMPTY, l(null), l("foo")).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Position(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Position(EMPTY, l(5), l("foo")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Position(EMPTY, l("foo bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae2.getMessage());
    }
    
    public void testConcat() {
        assertEquals("foobar", new Concat(EMPTY, l("foo"), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("fb", new Concat(EMPTY, l('f'), l('b')).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertEquals("foo", new Concat(EMPTY, l("foo"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("bar", new Concat(EMPTY, l(null), l("bar")).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Concat(EMPTY, l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Concat(EMPTY, l(5), l("foo")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Concat(EMPTY, l("foo bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae2.getMessage());
    }
    
    public void testReplace() {
        assertEquals("foobazbaz", new Replace(EMPTY, l("foobarbar"), l("bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobXrbXr", new Replace(EMPTY, l("foobarbar"), l('a'), l('X')).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("z", new Replace(EMPTY, l('f'), l('f'), l('z')).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertEquals("foobarbar", new Replace(EMPTY, l("foobarbar"), l("bar"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobarbar", new Replace(EMPTY, l("foobarbar"), l(null), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Replace(EMPTY, l(null), l("bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Replace(EMPTY, l(null), l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Replace(EMPTY, l(5), l("bar"), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Replace(EMPTY, l("foobarbar"), l(4), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [4]", siae2.getMessage());
        SqlIllegalArgumentException siae3 = expectThrows(SqlIllegalArgumentException.class, () -> new Replace(EMPTY, l("foobarbar"), l("bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [3]", siae3.getMessage());
    }
    
    public void testSubstring() {
        assertEquals("bar", new Substring(EMPTY, l("foobarbar"), l(4), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foo", new Substring(EMPTY, l("foobarbar"), l(1), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("baz", new Substring(EMPTY, l("foobarbaz"), l(7), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("f", new Substring(EMPTY, l('f'), l(1), l(1)).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertEquals("foobarbar", new Substring(EMPTY, l("foobarbar"), l(1), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobarbar", new Substring(EMPTY, l("foobarbar"), l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Substring(EMPTY, l(null), l(1), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Substring(EMPTY, l(null), l(null), l(null)).makeProcessorDefinition().asProcessor().process(null));
        
        assertEquals("foo", new Substring(EMPTY, l("foobarbar"), l(-5), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("barbar", new Substring(EMPTY, l("foobarbar"), l(4), l(30)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("r", new Substring(EMPTY, l("foobarbar"), l(9), l(1)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(1), l(-3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(10), l(1)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("", new Substring(EMPTY, l("foobarbar"), l(123), l(3)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Substring(EMPTY, l(5), l(1), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Substring(EMPTY, l("foobarbar"), l(1), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [baz]", siae2.getMessage());
        SqlIllegalArgumentException siae3 = expectThrows(SqlIllegalArgumentException.class, () -> new Substring(EMPTY, l("foobarbar"), l("bar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [bar]", siae3.getMessage());
    }
    
    public void testLocate() {
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(7, new Locate(EMPTY, l("bar"), l("foobarbar"), l(5)).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertNull(new Locate(EMPTY, l("bar"), l(null), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(0, new Locate(EMPTY, l(null), l("foobarbar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(0, new Locate(EMPTY, l(null), l("foobarbar"), l(null)).makeProcessorDefinition().asProcessor().process(null));
        
        assertEquals(1, new Locate(EMPTY, l("foo"), l("foobarbar")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(1, new Locate(EMPTY, l('o'), l('o')).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(9, new Locate(EMPTY, l('r'), l("foobarbar"), l(9)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals(4, new Locate(EMPTY, l("bar"), l("foobarbar"), l(-3)).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Locate(EMPTY, l(5), l("foobarbar"), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Locate(EMPTY, l("foo"), l(1), l(3)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [1]", siae2.getMessage());
        SqlIllegalArgumentException siae3 = expectThrows(SqlIllegalArgumentException.class, () -> new Locate(EMPTY, l("foobarbar"), l("bar"), l('c')).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [c]", siae3.getMessage());
    }
    
    public void testInsert() {
        assertEquals("bazbar", new Insert(EMPTY, l("foobar"), l(1), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(4), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        
        // test the NULL and edge case situations
        assertNull(new Insert(EMPTY, l(null), l(4), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobar", new Insert(EMPTY, l("foobar"), l(4), l(3), l(null)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobar", new Insert(EMPTY, l("foobar"), l(null), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobar", new Insert(EMPTY, l("foobar"), l(4), l(null), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("bazbar", new Insert(EMPTY, l("foobar"), l(-1), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(4), l(30), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("foobaz", new Insert(EMPTY, l("foobar"), l(6), l(1), l('z')).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("z", new Insert(EMPTY, l('f'), l(1), l(10), l('z')).makeProcessorDefinition().asProcessor().process(null));
        
        // validate the inputs
        SqlIllegalArgumentException siae1 = expectThrows(SqlIllegalArgumentException.class, () -> new Insert(EMPTY, l(5), l(1), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [5]", siae1.getMessage());
        SqlIllegalArgumentException siae2 = expectThrows(SqlIllegalArgumentException.class, () -> new Insert(EMPTY, l("foobar"), l(1), l(3), l(66)).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A string/char is required; received [66]", siae2.getMessage());
        SqlIllegalArgumentException siae3 = expectThrows(SqlIllegalArgumentException.class, () -> new Insert(EMPTY, l("foobar"), l("c"), l(3), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [c]", siae3.getMessage());
        SqlIllegalArgumentException siae4 = expectThrows(SqlIllegalArgumentException.class, () -> new Insert(EMPTY, l("foobar"), l(1), l('z'), l("baz")).makeProcessorDefinition().asProcessor().process(null));
        assertEquals("A number is required; received [z]", siae4.getMessage());
    }
    
    private static Literal l(Object value) {
        return Literal.of(EMPTY, value);
    }
}
