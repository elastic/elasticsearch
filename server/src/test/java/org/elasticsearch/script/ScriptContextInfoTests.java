/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptContextInfo.ScriptMethodInfo;
import org.elasticsearch.script.ScriptContextInfo.ScriptMethodInfo.ParameterInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScriptContextInfoTests extends ESTestCase {
    public interface MinimalContext {
        void execute();
    }

    public void testMinimalContext() {
        String name = "minimal_context";
        ScriptContextInfo info = new ScriptContextInfo(name, MinimalContext.class);
        assertEquals(name, info.name);
        assertEquals("execute", info.execute.name);
        assertEquals("void", info.execute.returnType);
        assertEquals(0, info.execute.parameters.size());
        assertEquals(0, info.getters.size());
    }

    public static class PrimitiveContext {
        public int execute(boolean foo, long bar, short baz, float qux) {return 0;}
        public static final String[] PARAMETERS = {"foo", "bar", "baz", "qux"};
        public byte getByte() {return 0x00;}
        public char getChar() {return 'a';}
    }

    public void testPrimitiveContext() {
        String name = "primitive_context";
        ScriptContextInfo info = new ScriptContextInfo(name, PrimitiveContext.class);
        assertEquals(name, info.name);
        assertEquals("execute", info.execute.name);
        assertEquals("int", info.execute.returnType);
        assertEquals(4, info.execute.parameters.size());
        List<Tuple<String, String>> eparams = new ArrayList<>();
        eparams.add(new Tuple<>("boolean", "foo"));
        eparams.add(new Tuple<>("long", "bar"));
        eparams.add(new Tuple<>("short", "baz"));
        eparams.add(new Tuple<>("float", "qux"));
        for (int i=0; i < info.execute.parameters.size(); i++) {
            assertEquals(eparams.get(i).v1(), info.execute.parameters.get(i).type);
            assertEquals(eparams.get(i).v2(), info.execute.parameters.get(i).name);
        }
        assertEquals(2, info.getters.size());
        HashMap<String,String> getters = new HashMap<>(Map.of("getByte","byte", "getChar","char"));
        for (ScriptContextInfo.ScriptMethodInfo getter: info.getters) {
            assertEquals(0, getter.parameters.size());
            String returnType = getters.remove(getter.name);
            assertNotNull(returnType);
            assertEquals(returnType, getter.returnType);
        }
        assertEquals(0, getters.size());
    }


    public static class CustomType0 {}
    public static class CustomType1 {}
    public static class CustomType2 {}

    public static class CustomTypeContext {
        public CustomType0 execute(CustomType1 custom1, CustomType2 custom2) {return new CustomType0();}
        public static final String[] PARAMETERS = {"custom1", "custom2"};
        public CustomType1 getCustom1() {return new CustomType1();}
        public CustomType2 getCustom2() {return new CustomType2();}
    }

    public void testCustomTypeContext() {
        String ct = "org.elasticsearch.script.ScriptContextInfoTests$CustomType";
        String ct0 = ct + 0;
        String ct1 = ct + 1;
        String ct2 = ct + 2;
        String name = "custom_type_context";
        ScriptContextInfo info = new ScriptContextInfo(name, CustomTypeContext.class);
        assertEquals(name, info.name);
        assertEquals("execute", info.execute.name);
        assertEquals(ct0, info.execute.returnType);
        assertEquals(2, info.execute.parameters.size());
        List<Tuple<String, String>> eparams = new ArrayList<>();
        eparams.add(new Tuple<>(ct1, "custom1"));
        eparams.add(new Tuple<>(ct2, "custom2"));
        for (int i=0; i < info.execute.parameters.size(); i++) {
            assertEquals(eparams.get(i).v1(), info.execute.parameters.get(i).type);
            assertEquals(eparams.get(i).v2(), info.execute.parameters.get(i).name);
        }
        assertEquals(2, info.getters.size());
        HashMap<String,String> getters = new HashMap<>(Map.of("getCustom1",ct1, "getCustom2",ct2));
        for (ScriptContextInfo.ScriptMethodInfo getter: info.getters) {
            assertEquals(0, getter.parameters.size());
            String returnType = getters.remove(getter.name);
            assertNotNull(returnType);
            assertEquals(returnType, getter.returnType);
        }
        assertEquals(0, getters.size());

        HashMap<String,String> methods = new HashMap<>(Map.of("getCustom1",ct1, "getCustom2",ct2, "execute",ct0));
        for (ScriptContextInfo.ScriptMethodInfo method: info.methods()) {
            String returnType = methods.remove(method.name);
            assertNotNull(returnType);
            assertEquals(returnType, method.returnType);
        }
        assertEquals(0, methods.size());
    }

    public static class TwoExecute {
        public void execute(int foo) {}
        public boolean execute(boolean foo) {return foo;}
        public static final String[] PARAMETERS = {"foo"};
    }

    public void testTwoExecute() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("two_execute", TwoExecute.class));
        assertEquals("Cannot have multiple [execute] methods on class [" + TwoExecute.class.getName() + "]", e.getMessage());
    }

    public static class NoExecute {}

    public void testNoExecute() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("no_execute", NoExecute.class));
        assertEquals("Could not find required method [execute] on class [" + NoExecute.class.getName() + "]", e.getMessage());
    }

    public static class NoParametersField {
        public void execute(int foo) {}
    }

    public void testNoParametersField() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("no_parameters_field", NoParametersField.class));
        assertEquals("Could not find field [PARAMETERS] on instance class [" + NoParametersField.class.getName() +
            "] but method [execute] has [1] parameters", e.getMessage());
    }

    public static class BadParametersFieldType {
        public void execute(int foo) {}
        public static final int[] PARAMETERS = {1};
    }

    public void testBadParametersFieldType() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("bad_parameters_field_type", BadParametersFieldType.class));
        assertEquals("Expected a constant [String[] PARAMETERS] on instance class [" + BadParametersFieldType.class.getName() +
            "] for method [execute] with [1] parameters, found [int[]]", e.getMessage());
    }

    public static class WrongNumberOfParameters {
        public void execute(int foo) {}
        public static final String[] PARAMETERS = {"foo", "bar"};
    }

    public void testWrongNumberOfParameters() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("wrong_number_of_parameters", WrongNumberOfParameters.class));
        assertEquals("Expected argument names [2] to have the same arity [1] for method [execute] of class ["
            + WrongNumberOfParameters.class.getName() + "]", e.getMessage());
    }

    public interface Default {
        default int getDefault() {return 1;}
        boolean getNonDefault1();
    }

    public static class GetterConditional implements Default {
        public void execute() {}
        public boolean getNonDefault1() {return true;}
        public float getNonDefault2() {return 0.1f;}
        public static long getStatic() {return 2L;}
        public char getChar(char ch) { return ch;}
    }

    public void testGetterConditional() {
        Set<ScriptMethodInfo> getters =
            new ScriptContextInfo("getter_conditional", GetterConditional.class).getters;
        assertEquals(2, getters.size());
        HashMap<String,String> methods = new HashMap<>(Map.of("getNonDefault1","boolean", "getNonDefault2","float"));
        for (ScriptContextInfo.ScriptMethodInfo method: getters) {
            String returnType = methods.remove(method.name);
            assertNotNull(returnType);
            assertEquals(returnType, method.returnType);
        }
        assertEquals(0, methods.size());
    }

    public void testParameterInfoParser() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        XContentParser parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray("{\"type\":\"foo\", \"name\": \"bar\"}").streamInput());
        ScriptContextInfo.ScriptMethodInfo.ParameterInfo info = ScriptContextInfo.ScriptMethodInfo.ParameterInfo.fromXContent(parser);
        assertEquals(new ScriptContextInfo.ScriptMethodInfo.ParameterInfo("foo", "bar"), info);
    }

    public void testScriptMethodInfoParser() throws IOException {
        String json = "{\"name\": \"fooFunc\", \"return_type\": \"int\", \"params\": [{\"type\": \"int\", \"name\": \"fooParam\"}, " +
            "{\"type\": \"java.util.Map\", \"name\": \"barParam\"}]}";
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(json).streamInput());
        ScriptContextInfo.ScriptMethodInfo info = ScriptContextInfo.ScriptMethodInfo.fromXContent(parser);
        assertEquals(new ScriptContextInfo.ScriptMethodInfo("fooFunc", "int", new ArrayList<>(
            Arrays.asList(new ScriptContextInfo.ScriptMethodInfo.ParameterInfo("int", "fooParam"),
                new ScriptContextInfo.ScriptMethodInfo.ParameterInfo("java.util.Map", "barParam"))
        )), info);
    }

    public void testScriptContextInfoParser() throws IOException {
        String json = "{" +
            "  \"name\": \"similarity\"," +
            "  \"methods\": [" +
            "    {" +
            "      \"name\": \"execute\"," +
            "      \"return_type\": \"double\"," +
            "      \"params\": [" +
            "        {" +
            "          \"type\": \"double\"," +
            "          \"name\": \"weight\"" +
            "        }," +
            "        {" +
            "          \"type\": \"org.elasticsearch.index.similarity.ScriptedSimilarity$Query\"," +
            "          \"name\": \"query\"" +
            "        }," +
            "        {" +
            "          \"type\": \"org.elasticsearch.index.similarity.ScriptedSimilarity$Field\"," +
            "          \"name\": \"field\"" +
            "        }," +
            "        {" +
            "          \"type\": \"org.elasticsearch.index.similarity.ScriptedSimilarity$Term\"," +
            "          \"name\": \"term\"" +
            "        }," +
            "        {" +
            "          \"type\": \"org.elasticsearch.index.similarity.ScriptedSimilarity$Doc\"," +
            "          \"name\": \"doc\"" +
            "        }" +
            "      ]" +
            "    }," +
            "    {" +
            "      \"name\": \"getParams\"," +
            "      \"return_type\": \"java.util.Map\"," +
            "      \"params\": []" +
            "    }," +
            "    {" +
            "      \"name\": \"getDoc\"," +
            "      \"return_type\": \"java.util.Map\"," +
            "      \"params\": []" +
            "    }," +
            "    {" +
            "      \"name\": \"get_score\"," +
            "      \"return_type\": \"double\"," +
            "      \"params\": []" +
            "    }" +
            "  ]" +
            "}";
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(
                NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(json).streamInput());
        ScriptContextInfo parsed = ScriptContextInfo.fromXContent(parser);
        ScriptContextInfo expected = new ScriptContextInfo(
            "similarity",
            new ScriptMethodInfo(
                "execute",
                "double",
                List.of(
                    new ParameterInfo("double", "weight"),
                    new ParameterInfo("org.elasticsearch.index.similarity.ScriptedSimilarity$Query", "query"),
                    new ParameterInfo("org.elasticsearch.index.similarity.ScriptedSimilarity$Field", "field"),
                    new ParameterInfo("org.elasticsearch.index.similarity.ScriptedSimilarity$Term", "term"),
                    new ParameterInfo("org.elasticsearch.index.similarity.ScriptedSimilarity$Doc", "doc")
                )
            ),
            Set.of(
                new ScriptMethodInfo("getParams", "java.util.Map", new ArrayList<>()),
                new ScriptMethodInfo("getDoc", "java.util.Map", new ArrayList<>()),
                new ScriptMethodInfo("get_score", "double", new ArrayList<>())
            )
        );
        assertEquals(expected, parsed);
    }

    public void testIgnoreOtherMethodsInListConstructor() {
        ScriptContextInfo constructed = new ScriptContextInfo("otherNames", List.of(
            new ScriptMethodInfo("execute", "double", Collections.emptyList()),
            new ScriptMethodInfo("otherName", "bool", Collections.emptyList())
        ));
        ScriptContextInfo expected = new ScriptContextInfo("otherNames",
            new ScriptMethodInfo("execute", "double", Collections.emptyList()),
            Collections.emptySet()
        );
        assertEquals(expected, constructed);
    }

    public void testNoExecuteInListConstructor() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("noExecute", List.of(
                new ScriptMethodInfo("getSomeOther", "int", Collections.emptyList()),
                new ScriptMethodInfo("getSome", "bool", Collections.emptyList())
            )));
        assertEquals("Could not find required method [execute] in [noExecute], found [getSome, getSomeOther]", e.getMessage());
    }

    public void testMultipleExecuteInListConstructor() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ScriptContextInfo("multiexecute", List.of(
                new ScriptMethodInfo("execute", "double", Collections.emptyList()),
                new ScriptMethodInfo("execute", "double", List.of(
                    new ParameterInfo("double", "weight")
            )))));
        assertEquals("Cannot have multiple [execute] methods in [multiexecute], found [2]", e.getMessage());
    }
}
