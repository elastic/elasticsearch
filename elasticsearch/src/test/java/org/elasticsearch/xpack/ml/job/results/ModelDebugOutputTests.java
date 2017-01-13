/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

import java.util.Date;

public class ModelDebugOutputTests extends AbstractSerializingTestCase<ModelDebugOutput> {

    @Override
    protected ModelDebugOutput createTestInstance() {
        return createTestInstance("foo");
    }

    public ModelDebugOutput createTestInstance(String jobId) {
        ModelDebugOutput modelDebugOutput = new ModelDebugOutput(jobId);
        if (randomBoolean()) {
            modelDebugOutput.setByFieldName(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelDebugOutput.setByFieldValue(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelDebugOutput.setPartitionFieldName(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelDebugOutput.setPartitionFieldValue(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelDebugOutput.setDebugFeature(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelDebugOutput.setDebugLower(randomDouble());
        }
        if (randomBoolean()) {
            modelDebugOutput.setDebugUpper(randomDouble());
        }
        if (randomBoolean()) {
            modelDebugOutput.setDebugMedian(randomDouble());
        }
        if (randomBoolean()) {
            modelDebugOutput.setActual(randomDouble());
        }
        if (randomBoolean()) {
            modelDebugOutput.setTimestamp(new Date(randomLong()));
        }
        return modelDebugOutput;
    }

    @Override
    protected Reader<ModelDebugOutput> instanceReader() {
        return ModelDebugOutput::new;
    }

    @Override
    protected ModelDebugOutput parseInstance(XContentParser parser) {
        return ModelDebugOutput.PARSER.apply(parser, null);
    }

    public void testEquals_GivenSameObject() {
        ModelDebugOutput modelDebugOutput = new ModelDebugOutput(randomAsciiOfLength(15));

        assertTrue(modelDebugOutput.equals(modelDebugOutput));
    }

    public void testEquals_GivenObjectOfDifferentClass() {
        ModelDebugOutput modelDebugOutput = new ModelDebugOutput(randomAsciiOfLength(15));

        assertFalse(modelDebugOutput.equals("a string"));
    }

    public void testEquals_GivenDifferentTimestamp() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setTimestamp(new Date(0L));

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentPartitionFieldName() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setPartitionFieldName("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentPartitionFieldValue() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setPartitionFieldValue("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentByFieldName() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setByFieldName("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentByFieldValue() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setByFieldValue("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentOverFieldName() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setOverFieldName("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentOverFieldValue() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setOverFieldValue("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentDebugFeature() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setDebugFeature("another");

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentDebugLower() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setDebugLower(-1.0);

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentDebugUpper() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setDebugUpper(-1.0);

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentDebugMean() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setDebugMedian(-1.0);

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenDifferentActual() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();
        modelDebugOutput2.setActual(-1.0);

        assertFalse(modelDebugOutput1.equals(modelDebugOutput2));
        assertFalse(modelDebugOutput2.equals(modelDebugOutput1));
    }

    public void testEquals_GivenEqualModelDebugOutputs() {
        ModelDebugOutput modelDebugOutput1 = createFullyPopulated();
        ModelDebugOutput modelDebugOutput2 = createFullyPopulated();

        assertTrue(modelDebugOutput1.equals(modelDebugOutput2));
        assertTrue(modelDebugOutput2.equals(modelDebugOutput1));
        assertEquals(modelDebugOutput1.hashCode(), modelDebugOutput2.hashCode());
    }

    private ModelDebugOutput createFullyPopulated() {
        ModelDebugOutput modelDebugOutput = new ModelDebugOutput("foo");
        modelDebugOutput.setByFieldName("by");
        modelDebugOutput.setByFieldValue("by_val");
        modelDebugOutput.setPartitionFieldName("part");
        modelDebugOutput.setPartitionFieldValue("part_val");
        modelDebugOutput.setDebugFeature("sum");
        modelDebugOutput.setDebugLower(7.9);
        modelDebugOutput.setDebugUpper(34.5);
        modelDebugOutput.setDebugMedian(12.7);
        modelDebugOutput.setActual(100.0);
        modelDebugOutput.setTimestamp(new Date(12345678L));
        return modelDebugOutput;
    }

}
