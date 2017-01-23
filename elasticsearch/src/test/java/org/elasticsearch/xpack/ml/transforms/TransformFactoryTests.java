/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;

import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.config.transform.TransformType;
import org.elasticsearch.xpack.ml.transforms.Transform.TransformIndex;

public class TransformFactoryTests extends ESTestCase {

    public void testIndexesMapping() {
        TransformConfig conf = new TransformConfig(TransformType.CONCAT.prettyName());
        conf.setInputs(Arrays.asList("field1", "field2"));
        conf.setOutputs(Arrays.asList("concatted"));

        Map<String, Integer> inputMap = new HashMap<>();
        inputMap.put("field1", 5);
        inputMap.put("field2", 3);

        Map<String, Integer> scratchMap = new HashMap<>();

        Map<String, Integer> outputMap = new HashMap<>();
        outputMap.put("concatted", 2);

        Transform tr = new TransformFactory().create(conf, inputMap, scratchMap,
                outputMap, mock(Logger.class));
        assertTrue(tr instanceof Concat);

        List<TransformIndex> inputIndexes = tr.getReadIndexes();
        assertEquals(inputIndexes.get(0), new TransformIndex(0, 5));
        assertEquals(inputIndexes.get(1), new TransformIndex(0, 3));

        List<TransformIndex> outputIndexes = tr.getWriteIndexes();
        assertEquals(outputIndexes.get(0), new TransformIndex(2, 2));
    }

    public void testConcatWithOptionalArgs() {
        TransformConfig conf = new TransformConfig(TransformType.CONCAT.prettyName());
        conf.setInputs(Arrays.asList("field1", "field2"));
        conf.setOutputs(Arrays.asList("concatted"));

        Map<String, Integer> inputMap = new HashMap<>();
        inputMap.put("field1", 5);
        inputMap.put("field2", 3);

        Map<String, Integer> scratchMap = new HashMap<>();

        Map<String, Integer> outputMap = new HashMap<>();
        outputMap.put("concatted", 2);

        Transform tr = new TransformFactory().create(conf, inputMap, scratchMap,
                outputMap, mock(Logger.class));
        assertTrue(tr instanceof Concat);
        assertEquals("", ((Concat) tr).getDelimiter());

        conf.setArguments(Arrays.asList("delimiter"));
        tr = new TransformFactory().create(conf, inputMap, scratchMap,
                outputMap, mock(Logger.class));
        assertTrue(tr instanceof Concat);
        assertEquals("delimiter", ((Concat) tr).getDelimiter());
    }

    public void testAllTypesCreated() {
        EnumSet<TransformType> all = EnumSet.allOf(TransformType.class);

        Map<String, Integer> inputIndexes = new HashMap<>();
        Map<String, Integer> scratchMap = new HashMap<>();
        Map<String, Integer> outputIndexes = new HashMap<>();

        for (TransformType type : all) {
            TransformConfig conf = TransformTestUtils.createValidTransform(type);
            conf.getInputs().stream().forEach(input -> inputIndexes.put(input, 0));
            conf.getOutputs().stream().forEach(output -> outputIndexes.put(output, 0));

            // throws IllegalArgumentException if it doesn't handle the type
            new TransformFactory().create(conf, inputIndexes, scratchMap,
                    outputIndexes, mock(Logger.class));
        }
    }

    public void testExcludeTransformsCreated() {
        Map<String, Integer> inputIndexes = new HashMap<>();
        Map<String, Integer> scratchMap = new HashMap<>();
        Map<String, Integer> outputIndexes = new HashMap<>();


        TransformConfig conf = new TransformConfig(TransformType.EXCLUDE.prettyName());
        conf.setInputs(new ArrayList<>());
        conf.setOutputs(new ArrayList<>());
        conf.setCondition(new Condition(Operator.LT, "2000"));


        ExcludeFilterNumeric numericTransform =
                (ExcludeFilterNumeric) new TransformFactory().create(conf, inputIndexes,
                        scratchMap, outputIndexes, mock(Logger.class));

        assertEquals(Operator.LT, numericTransform.getCondition().getOperator());
        assertEquals(2000, numericTransform.filterValue(), 0.0000001);

        conf.setCondition(new Condition(Operator.MATCH, "aaaaa"));

        ExcludeFilterRegex regexTransform =
                (ExcludeFilterRegex) new TransformFactory().create(conf, inputIndexes,
                        scratchMap, outputIndexes, mock(Logger.class));

        assertEquals(Operator.MATCH, regexTransform.getCondition().getOperator());
        assertEquals("aaaaa", regexTransform.getCondition().getValue());
    }

}
