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

public class ModelPlotTests extends AbstractSerializingTestCase<ModelPlot> {

    @Override
    protected ModelPlot createTestInstance() {
        return createTestInstance("foo");
    }

    public ModelPlot createTestInstance(String jobId) {
        ModelPlot modelPlot = new ModelPlot(jobId);
        if (randomBoolean()) {
            modelPlot.setByFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelPlot.setByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelPlot.setPartitionFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelPlot.setPartitionFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelPlot.setModelFeature(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            modelPlot.setModelLower(randomDouble());
        }
        if (randomBoolean()) {
            modelPlot.setModelUpper(randomDouble());
        }
        if (randomBoolean()) {
            modelPlot.setModelMedian(randomDouble());
        }
        if (randomBoolean()) {
            modelPlot.setActual(randomDouble());
        }
        if (randomBoolean()) {
            modelPlot.setTimestamp(new Date(randomLong()));
        }
        return modelPlot;
    }

    @Override
    protected Reader<ModelPlot> instanceReader() {
        return ModelPlot::new;
    }

    @Override
    protected ModelPlot parseInstance(XContentParser parser) {
        return ModelPlot.PARSER.apply(parser, null);
    }

    public void testEquals_GivenSameObject() {
        ModelPlot modelPlot = new ModelPlot(randomAlphaOfLength(15));

        assertTrue(modelPlot.equals(modelPlot));
    }

    public void testEquals_GivenObjectOfDifferentClass() {
        ModelPlot modelPlot = new ModelPlot(randomAlphaOfLength(15));

        assertFalse(modelPlot.equals("a string"));
    }

    public void testEquals_GivenDifferentTimestamp() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setTimestamp(new Date(0L));

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentPartitionFieldName() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setPartitionFieldName("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentPartitionFieldValue() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setPartitionFieldValue("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentByFieldName() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setByFieldName("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentByFieldValue() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setByFieldValue("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentOverFieldName() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setOverFieldName("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentOverFieldValue() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setOverFieldValue("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentModelFeature() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setModelFeature("another");

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentModelLower() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setModelLower(-1.0);

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentModelUpper() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setModelUpper(-1.0);

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentModelMean() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setModelMedian(-1.0);

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenDifferentActual() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setActual(-1.0);

        assertFalse(modelPlot1.equals(modelPlot2));
        assertFalse(modelPlot2.equals(modelPlot1));
    }

    public void testEquals_GivenEqualmodelPlots() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();

        assertTrue(modelPlot1.equals(modelPlot2));
        assertTrue(modelPlot2.equals(modelPlot1));
        assertEquals(modelPlot1.hashCode(), modelPlot2.hashCode());
    }

    private ModelPlot createFullyPopulated() {
        ModelPlot modelPlot = new ModelPlot("foo");
        modelPlot.setByFieldName("by");
        modelPlot.setByFieldValue("by_val");
        modelPlot.setPartitionFieldName("part");
        modelPlot.setPartitionFieldValue("part_val");
        modelPlot.setModelFeature("sum");
        modelPlot.setModelLower(7.9);
        modelPlot.setModelUpper(34.5);
        modelPlot.setModelMedian(12.7);
        modelPlot.setActual(100.0);
        modelPlot.setTimestamp(new Date(12345678L));
        return modelPlot;
    }

}
