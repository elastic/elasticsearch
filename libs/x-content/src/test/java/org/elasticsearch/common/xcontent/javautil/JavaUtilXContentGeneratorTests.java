package org.elasticsearch.common.xcontent.javautil;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

public class JavaUtilXContentGeneratorTests extends ESTestCase {

    public void testReturnsNullResultWhenNoDataAdded() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();
        assertEquals(gen.getResult(), null);
    }

    @SuppressWarnings("unchecked")
    public void testCanCreateSimpleObject() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();

        gen.writeStartObject();
        gen.writeStringField("foo", "bar");
        gen.writeNumberField("fiz", 123);
        gen.writeFieldName("bool");
        gen.writeBoolean(false);
        gen.writeNullField("lol");
        gen.writeEndObject();

        Map<String, Object> res = (Map<String, Object>) gen.getResult();
        assertEquals(res.get("foo"), "bar");
        assertEquals(res.get("fiz"), 123);
        assertEquals(res.get("bool"), false);
        assertEquals(res.get("lol"), null);
    }

    @SuppressWarnings("unchecked")
    public void testCanCreateNextedObjects() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();

        gen.writeStartObject();
        gen.writeStringField("foo", "bar");
        gen.writeFieldName("fiz");
        gen.writeStartObject();
        gen.writeFieldName("hello");
        gen.writeNumber(123);
        gen.writeEndObject();
        gen.writeEndObject();

        Map<String, Object> res = (Map<String, Object>) gen.getResult();
        assertEquals(res.get("foo"), "bar");
        assertEquals(((Map<String, Object>)res.get("fiz")).get("hello"), 123);
    }


    public void testThrowsWhenAddingFieldsToNonExistingObject() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();

        expectThrows(IOException.class, () -> gen.writeStringField("foo", "bar"));
    }

    public void testThrowsWhenAccessingResultOfUnfinishedObject() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();

        gen.writeStartObject();
        gen.writeStringField("foo", "bar");

        expectThrows(Exception.class, () -> gen.getResult());
    }
}
