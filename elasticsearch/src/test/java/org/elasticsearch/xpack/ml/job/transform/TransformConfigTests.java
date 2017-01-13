/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.transform;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.condition.Condition;
import org.elasticsearch.xpack.ml.job.condition.Operator;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class TransformConfigTests extends AbstractSerializingTestCase<TransformConfig> {

    @Override
    protected TransformConfig createTestInstance() {
        TransformType transformType = randomFrom(TransformType.values());
        TransformConfig config = new TransformConfig(transformType.prettyName());
        if (randomBoolean()) {
            config.setInputs(Arrays.asList(generateRandomStringArray(0, 10, false)));
        }
        if (randomBoolean()) {
            config.setOutputs(Arrays.asList(generateRandomStringArray(0, 10, false)));
        }
        if (randomBoolean()) {
            config.setArguments(Arrays.asList(generateRandomStringArray(0, 10, false)));
        }
        if (randomBoolean()) {
            // no need to randomize, it is properly randomily tested in ConditionTest
            config.setCondition(new Condition(Operator.LT, Double.toString(randomDouble())));
        }
        return config;
    }

    @Override
    protected Writeable.Reader<TransformConfig> instanceReader() {
        return TransformConfig::new;
    }

    @Override
    protected TransformConfig parseInstance(XContentParser parser) {
        return TransformConfig.PARSER.apply(parser, null);
    }

    public void testGetOutputs_GivenNoExplicitOutputsSpecified() {
        TransformConfig config = new TransformConfig("concat");

        assertEquals(Arrays.asList("concat"), config.getOutputs());
    }

    public void testGetOutputs_GivenEmptyOutputsSpecified() {
        TransformConfig config = new TransformConfig("concat");
        assertEquals(Arrays.asList("concat"), config.getOutputs());
    }


    public void testGetOutputs_GivenOutputsSpecified() {
        TransformConfig config = new TransformConfig("concat");
        config.setOutputs(Arrays.asList("o1", "o2"));

        assertEquals(Arrays.asList("o1", "o2"), config.getOutputs());
    }


    public void testVerify_GivenUnknownTransform() {
        ESTestCase.expectThrows(IllegalArgumentException.class, () -> new TransformConfig("unknown+transform"));
    }


    public void testEquals_GivenSameReference() {
        TransformConfig config = new TransformConfig(TransformType.CONCAT.prettyName());
        assertTrue(config.equals(config));
    }


    public void testEquals_GivenDifferentClass() {
        TransformConfig config = new TransformConfig(TransformType.CONCAT.prettyName());
        assertFalse(config.equals("a string"));
    }


    public void testEquals_GivenNull() {
        TransformConfig config = new TransformConfig(TransformType.CONCAT.prettyName());
        assertFalse(config.equals(null));
    }


    public void testEquals_GivenEqualTransform() {
        TransformConfig config1 = new TransformConfig("concat");
        config1.setInputs(Arrays.asList("input1", "input2"));
        config1.setOutputs(Arrays.asList("output"));
        config1.setArguments(Arrays.asList("-"));
        config1.setCondition(new Condition(Operator.EQ, "5"));

        TransformConfig config2 = new TransformConfig("concat");
        config2.setInputs(Arrays.asList("input1", "input2"));
        config2.setOutputs(Arrays.asList("output"));
        config2.setArguments(Arrays.asList("-"));
        config2.setCondition(new Condition(Operator.EQ, "5"));

        assertTrue(config1.equals(config2));
        assertTrue(config2.equals(config1));
    }


    public void testEquals_GivenDifferentType() {
        TransformConfig config1 = new TransformConfig("concat");
        TransformConfig config2 = new TransformConfig("lowercase");
        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }


    public void testEquals_GivenDifferentInputs() {
        TransformConfig config1 = new TransformConfig("concat");
        config1.setInputs(Arrays.asList("input1", "input2"));

        TransformConfig config2 = new TransformConfig("concat");
        config2.setInputs(Arrays.asList("input1", "input3"));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }


    public void testEquals_GivenDifferentOutputs() {
        TransformConfig config1 = new TransformConfig("concat");
        config1.setInputs(Arrays.asList("input1", "input2"));
        config1.setOutputs(Arrays.asList("output1"));

        TransformConfig config2 = new TransformConfig("concat");
        config2.setInputs(Arrays.asList("input1", "input2"));
        config2.setOutputs(Arrays.asList("output2"));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }


    public void testEquals_GivenDifferentArguments() {
        TransformConfig config1 = new TransformConfig("concat");
        config1.setInputs(Arrays.asList("input1", "input2"));
        config1.setOutputs(Arrays.asList("output"));
        config1.setArguments(Arrays.asList("-"));

        TransformConfig config2 = new TransformConfig("concat");
        config2.setInputs(Arrays.asList("input1", "input2"));
        config2.setOutputs(Arrays.asList("output"));
        config2.setArguments(Arrays.asList("--"));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }


    public void testEquals_GivenDifferentConditions() {
        TransformConfig config1 = new TransformConfig("concat");
        config1.setInputs(Arrays.asList("input1", "input2"));
        config1.setOutputs(Arrays.asList("output"));
        config1.setArguments(Arrays.asList("-"));
        config1.setCondition(new Condition(Operator.MATCH, "foo"));

        TransformConfig config2 = new TransformConfig("concat");
        config2.setInputs(Arrays.asList("input1", "input2"));
        config2.setOutputs(Arrays.asList("output"));
        config2.setArguments(Arrays.asList("-"));
        config2.setCondition(new Condition(Operator.MATCH, "bar"));

        assertFalse(config1.equals(config2));
        assertFalse(config2.equals(config1));
    }

    public void testInvalidTransformName() throws Exception {
        BytesArray json = new BytesArray("{ \"transform\":\"\" }");
        XContentParser parser = XContentFactory.xContent(json).createParser(NamedXContentRegistry.EMPTY, json);
        ParsingException ex = expectThrows(ParsingException.class,
                () -> TransformConfig.PARSER.apply(parser, null));
        assertThat(ex.getMessage(), containsString("[transform] failed to parse field [transform]"));
        Throwable cause = ex.getRootCause();
        assertNotNull(cause);
        assertThat(cause, instanceOf(IllegalArgumentException.class));
        assertThat(cause.getMessage(),
                containsString("Unknown [transformType]: []"));
    }
}
