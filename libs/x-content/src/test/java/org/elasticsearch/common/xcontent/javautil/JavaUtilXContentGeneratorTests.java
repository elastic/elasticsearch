package org.elasticsearch.common.xcontent.javautil;

import static org.elasticsearch.common.xcontent.javautil.JavaUtilXContentGenerator.toCamelCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
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

    @SuppressWarnings("unchecked")
    public void testCanInsertObjectIntoArray() throws Exception {
        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();

        gen.writeStartArray();
        gen.writeStartObject();
        gen.writeStringField("foo", "bar");
        gen.writeEndObject();
        gen.writeEndArray();

        List<Object> list = (List<Object>) gen.getResult();
        Map<String, Object> map = (Map<String, Object>) list.get(0);
        assertEquals(map.get("foo"), "bar");
    }

//    @SuppressWarnings("unchecked")
//    public void testForcesFieldNamesToCamelCase() throws Exception {
//        JavaUtilXContentGenerator gen = new JavaUtilXContentGenerator();
//
//        gen.writeStartObject();
//        gen.writeStringField("helloWorld", "1");
//        gen.writeStringField("cluster_size", "2");
//        gen.writeStringField("doc.count", "3");
//        gen.writeEndObject();
//
//        Map<String, Object> map = (Map<String, Object>) gen.getResult();
//        assertEquals("1", map.get("helloWorld"));
//        assertEquals("2", map.get("clusterSize"));
//        assertEquals("3", map.get("docCount"));
//    }

//    public void testCamelCasesStringsCorrectly() throws Exception {
//        assertEquals("test", toCamelCase("test"));
//        assertEquals("foobar", toCamelCase("foobar"));
//        assertEquals("fooBar", toCamelCase("foo.bar"));
//        assertEquals("fooBar", toCamelCase("foo_bar"));
//        assertEquals("fooBar", toCamelCase("foo-bar"));
//        assertEquals("fooBarBaz", toCamelCase("foo.bar.baz"));
//        assertEquals("fooBarBaz", toCamelCase("foo_bar_baz"));
//        assertEquals("fooBarBaz", toCamelCase("foo-bar-baz"));
//        assertEquals("security7", toCamelCase(".security-7"));
//    }
}
