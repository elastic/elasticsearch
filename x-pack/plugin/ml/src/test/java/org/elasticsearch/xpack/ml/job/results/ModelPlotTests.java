/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.job.results.ModelPlot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ModelPlotTests extends AbstractSerializingTestCase<ModelPlot> {

    @Override
    protected ModelPlot createTestInstance() {
        return createTestInstance("foo");
    }

    public ModelPlot createTestInstance(String jobId) {
        ModelPlot modelPlot = new ModelPlot(jobId, randomDate(), randomNonNegativeLong(), randomInt());
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
        return modelPlot;
    }

    @Override
    protected Reader<ModelPlot> instanceReader() {
        return ModelPlot::new;
    }

    @Override
    protected ModelPlot doParseInstance(XContentParser parser) {
        return ModelPlot.STRICT_PARSER.apply(parser, null);
    }

    public void testEquals_GivenSameObject() {
        ModelPlot modelPlot = new ModelPlot(randomAlphaOfLength(15), randomDate(), randomNonNegativeLong(), randomInt());

        assertTrue(modelPlot.equals(modelPlot));
    }

    public void testEquals_GivenObjectOfDifferentClass() {
        ModelPlot modelPlot = new ModelPlot(randomAlphaOfLength(15), randomDate(), randomNonNegativeLong(), randomInt());

        assertFalse(modelPlot.equals("a string"));
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

    public void testEquals_GivenDifferentOneNullActual() {
        ModelPlot modelPlot1 = createFullyPopulated();
        ModelPlot modelPlot2 = createFullyPopulated();
        modelPlot2.setActual(null);

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

    public void testToXContent_GivenNullActual() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();

        ModelPlot modelPlot = createFullyPopulated();
        modelPlot.setActual(null);
        modelPlot.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertThat(json, not(containsString("actual")));
    }

    public void testId() {
        ModelPlot plot = new ModelPlot("job-foo", new Date(100L), 60L, 33);
        String byFieldValue = null;
        String overFieldValue = null;
        String partitionFieldValue = null;

        assertEquals("job-foo_model_plot_100_60_33_0_0", plot.getId());

        if (randomBoolean()) {
            byFieldValue = randomAlphaOfLength(10);
            plot.setByFieldValue(byFieldValue);
        }
        if (randomBoolean()) {
            overFieldValue = randomAlphaOfLength(10);
            plot.setOverFieldValue(overFieldValue);
        }
        if (randomBoolean()) {
            partitionFieldValue = randomAlphaOfLength(10);
            plot.setPartitionFieldValue(partitionFieldValue);
        }

        String valuesPart = MachineLearningField.valuesToId(byFieldValue, overFieldValue, partitionFieldValue);
        assertEquals("job-foo_model_plot_100_60_33_" + valuesPart, plot.getId());
    }

    public void testStrictParser() throws IOException {
        String json = """
            {"job_id":"job_1", "timestamp":12354667, "bucket_span": 3600, "detector_index":3, "foo":"bar"}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ModelPlot.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = """
            {"job_id":"job_1", "timestamp":12354667, "bucket_span": 3600, "detector_index":3, "foo":"bar"}
            """;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            ModelPlot.LENIENT_PARSER.apply(parser, null);
        }
    }

    public void testIdUniqueness() {
        ModelPlot modelPlot = new ModelPlot("foo", new Date(), 3600, 0);

        String[] partitionFieldValues = {
            "730",
            "132",
            "358",
            "552",
            "888",
            "236",
            "224",
            "674",
            "438",
            "128",
            "722",
            "560",
            "228",
            "628",
            "226",
            "656" };
        String[] byFieldValues = {
            "S000",
            "S001",
            "S002",
            "S003",
            "S004",
            "S005",
            "S006",
            "S007",
            "S008",
            "S009",
            "S010",
            "S011",
            "S012",
            "S013",
            "S014",
            "S015",
            "S016",
            "S017",
            "S018",
            "S019",
            "S020",
            "S021",
            "S022",
            "S023",
            "S024",
            "S025",
            "S026",
            "S027",
            "S028",
            "S029",
            "S057",
            "S058",
            "S059",
            "M020",
            "M021",
            "M026",
            "M027",
            "M028",
            "M029",
            "M030",
            "M031",
            "M032",
            "M033",
            "M056",
            "M057",
            "M058",
            "M059",
            "M060",
            "M061",
            "M062",
            "M063",
            "M086",
            "M087",
            "M088",
            "M089",
            "M090",
            "M091",
            "M092",
            "M093",
            "M116",
            "M117",
            "M118",
            "M119",
            "L012",
            "L013",
            "L014",
            "L017",
            "L018",
            "L019",
            "L023",
            "L024",
            "L025",
            "L029",
            "L030",
            "L031" };

        Map<String, List<String>> uniqueIds = new HashMap<>();

        for (String partitionFieldValue : partitionFieldValues) {
            modelPlot.setPartitionFieldValue(partitionFieldValue);
            for (String byFieldValue : byFieldValues) {
                modelPlot.setByFieldValue(byFieldValue);
                String id = modelPlot.getId();
                uniqueIds.compute(id, (k, v) -> {
                    if (v == null) {
                        v = new ArrayList<>();
                    }
                    v.add(partitionFieldValue + "/" + byFieldValue);
                    if (v.size() > 1) {
                        logger.error("Duplicates for ID [" + id + "]: " + v);
                    }
                    return v;
                });
            }
        }

        assertEquals(partitionFieldValues.length * byFieldValues.length, uniqueIds.size());
    }

    private ModelPlot createFullyPopulated() {
        ModelPlot modelPlot = new ModelPlot("foo", new Date(12345678L), 360L, 22);
        modelPlot.setByFieldName("by");
        modelPlot.setByFieldValue("by_val");
        modelPlot.setPartitionFieldName("part");
        modelPlot.setPartitionFieldValue("part_val");
        modelPlot.setModelFeature("sum");
        modelPlot.setModelLower(7.9);
        modelPlot.setModelUpper(34.5);
        modelPlot.setModelMedian(12.7);
        modelPlot.setActual(100.0);
        return modelPlot;
    }
}
