/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_INT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.intValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * Testing different scenarios where the coordinating node predefines a list of requested output fields and the actual execution occurs on
 * a data node with a different version, where the evaluating function produces outputs that may not fully match the predefined list.
 */
public class TestCompoundOutputEvaluatorTests extends AbstractCompoundOutputEvaluatorTests {

    private static Map<String, Object> testFunction(String input) {
        Map<String, Object> result = new HashMap<>();
        String[] parts = input.split("-");
        for (String part : parts) {
            String[] entry = part.trim().split(":");
            if (entry.length != 2) {
                throw new IllegalArgumentException("Invalid input: " + input);
            }
            Object value;
            try {
                value = Integer.parseInt(entry[1]);
            } catch (NumberFormatException e) {
                value = entry[1];
            }
            result.put(entry[0], value);
        }
        return result;
    }

    @Override
    protected CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector(List<String> requestedFields) {
        return new TestFieldsCollector(requestedFields);
    }

    @Override
    protected String collectorSimpleName() {
        return TestFieldsCollector.class.getSimpleName();
    }

    @Override
    protected Map<String, Class<?>> getSupportedOutputFieldMappings() {
        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("field_a", String.class);
        mappings.put("field_b", Integer.class);
        mappings.put("field_c", String.class);
        return mappings;
    }

    protected static class TestFieldsCollector extends CompoundOutputEvaluator.OutputFieldsCollector {
        private BiConsumer<CompoundOutputEvaluator.RowOutput, String> fieldA = NOOP_STRING_COLLECTOR;
        private ObjIntConsumer<CompoundOutputEvaluator.RowOutput> fieldB = NOOP_INT_COLLECTOR;
        private BiConsumer<CompoundOutputEvaluator.RowOutput, String> fieldC = NOOP_STRING_COLLECTOR;

        TestFieldsCollector(SequencedCollection<String> outputFields) {
            super(outputFields.size());
            int index = 0;
            for (String fieldName : outputFields) {
                switch (fieldName) {
                    case "field_a" -> fieldA = stringValueCollector(index);
                    case "field_b" -> fieldB = intValueCollector(index, value -> value >= 0);
                    case "field_c" -> fieldC = stringValueCollector(index);
                }
                index++;
            }
        }

        public void fieldA(String value) {
            fieldA.accept(rowOutput, value);
        }

        public void fieldB(Integer value) {
            fieldB.accept(rowOutput, value);
        }

        public void fieldC(String value) {
            fieldC.accept(rowOutput, value);
        }

        @Override
        protected void evaluate(String input) {
            Map<String, Object> evaluationFunctionOutput = testFunction(input);
            try {
                fieldA((String) evaluationFunctionOutput.get("field_a"));
                Object valueB = evaluationFunctionOutput.get("field_b");
                valueB = valueB == null ? -1 : ((Number) valueB).intValue();
                fieldB((Integer) valueB);
                fieldC((String) evaluationFunctionOutput.get("field_c"));
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid input: " + input, e);
            }
        }
    }

    public void testMatchingOutput() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { "valueA", 2, "valueC" });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testPartialFieldsRequested_1() {
        List<String> requestedFields = List.of("field_a", "field_b");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { "valueA", 2 });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testPartialFieldsRequested_2() {
        List<String> requestedFields = List.of("field_b");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { 2 });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testUnsupportedField() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC-extra_field:extraValue");
        List<Object[]> expected = toExpected(new Object[] { "valueA", 2, "valueC" });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingField_1() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { null, 2, "valueC" });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingField_2() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("foo:1-field_b:2-bar:3");
        List<Object[]> expected = toExpected(new Object[] { null, 2, null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testMissingField_3() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("foo:1-bar:2-field_b:3-baz:4-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { null, 3, "valueC" });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testAllMissingFields() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("field_d:2-field_e:valueE");
        List<Object[]> expected = toExpected(new Object[] { null, null, null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testWrongFieldType() {
        List<String> requestedFields = List.of("field_a", "field_b", "field_c");
        List<String> input = List.of("field_a:1-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { null, null, null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testKnownAndUnknownFields() {
        List<String> requestedFields = List.of("field_a", "field_b", "unknown_field");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { "valueA", 2, null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    public void testOnlyUnknownFields() {
        List<String> requestedFields = List.of("unknown_field_a", "unknown_field_b");
        List<String> input = List.of("field_a:valueA-field_b:2-field_c:valueC");
        List<Object[]> expected = toExpected(new Object[] { null, null });
        evaluateAndCompare(input, requestedFields, expected);
    }

    private List<Object[]> toExpected(Object[] expected) {
        List<Object[]> result = new ArrayList<>(expected.length);
        for (Object o : expected) {
            result.add(new Object[] { o });
        }
        return result;
    }

    /*****************************************************************************************************
     * Implementing AbstractCompoundOutputEvaluatorTests methods for the OperatorTestCase framework
     *****************************************************************************************************/

    @Override
    protected List<String> getRequestedFieldsForSimple() {
        return List.of("field_a", "field_b", "field_c");
    }

    @Override
    protected List<String> getSampleInputForSimple() {
        return List.of("field_a:valueA-field_b:2-field_c:valueC", "field_c:valueC-field_b:3-field_d:valueD");
    }

    @Override
    protected List<Object[]> getExpectedOutputForSimple() {
        return List.of(new Object[] { "valueA", null }, new Object[] { 2, 3 }, new Object[] { "valueC", "valueC" });
    }
}
