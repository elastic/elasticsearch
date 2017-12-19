/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.RecordWriter;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DetectorTests extends AbstractSerializingTestCase<Detector> {

    public void testEquals_GivenEqual() {
        Detector.Builder builder = new Detector.Builder("mean", "field");
        builder.setByFieldName("by_field");
        builder.setOverFieldName("over_field");
        builder.setPartitionFieldName("partition");
        builder.setUseNull(false);
        Detector detector1 = builder.build();

        builder = new Detector.Builder("mean", "field");
        builder.setByFieldName("by_field");
        builder.setOverFieldName("over_field");
        builder.setPartitionFieldName("partition");
        builder.setUseNull(false);
        Detector detector2 = builder.build();

        assertTrue(detector1.equals(detector2));
        assertTrue(detector2.equals(detector1));
        assertEquals(detector1.hashCode(), detector2.hashCode());
    }

    public void testEquals_GivenDifferentDetectorDescription() {
        Detector detector1 = createDetector().build();
        Detector.Builder builder = createDetector();
        builder.setDetectorDescription("bar");
        Detector detector2 = builder.build();

        assertFalse(detector1.equals(detector2));
    }

    public void testEquals_GivenDifferentByFieldName() {
        Detector detector1 = createDetector().build();
        Detector detector2 = createDetector().build();

        assertEquals(detector1, detector2);

        Detector.Builder builder = new Detector.Builder(detector2);
        builder.setByFieldName("by2");
        Condition condition = new Condition(Operator.GT, "5");
        DetectionRule rule = new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "by2", "val", condition, null)))
                .setActions(RuleAction.FILTER_RESULTS).setTargetFieldName("over_field")
                .setTargetFieldValue("targetValue")
                .setConditionsConnective(Connective.AND)
                .build();
        builder.setRules(Collections.singletonList(rule));
        detector2 = builder.build();
        assertFalse(detector1.equals(detector2));
    }

    public void testExtractAnalysisFields() {
        Detector detector = createDetector().build();
        assertEquals(Arrays.asList("by_field", "over_field", "partition"), detector.extractAnalysisFields());
        Detector.Builder builder = new Detector.Builder(detector);
        builder.setPartitionFieldName(null);
        detector = builder.build();
        assertEquals(Arrays.asList("by_field", "over_field"), detector.extractAnalysisFields());
        builder = new Detector.Builder(detector);
        Condition condition = new Condition(Operator.GT, "5");
        DetectionRule rule = new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null)))
                .setActions(RuleAction.FILTER_RESULTS)
                .setTargetFieldName("over_field")
                .setTargetFieldValue("targetValue")
                .setConditionsConnective(Connective.AND)
                .build();
        builder.setRules(Collections.singletonList(rule));
        builder.setByFieldName(null);
        detector = builder.build();
        assertEquals(Collections.singletonList("over_field"), detector.extractAnalysisFields());
        builder = new Detector.Builder(detector);
        rule = new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null)))
                .setActions(RuleAction.FILTER_RESULTS)
                .setConditionsConnective(Connective.AND)
                .build();
        builder.setRules(Collections.singletonList(rule));
        builder.setOverFieldName(null);
        detector = builder.build();
        assertTrue(detector.extractAnalysisFields().isEmpty());
    }

    public void testExtractReferencedLists() {
        Detector.Builder builder = createDetector();
        builder.setRules(Arrays.asList(
                new DetectionRule.Builder(Collections.singletonList(RuleCondition.createCategorical("by_field", "list1"))).build(),
                new DetectionRule.Builder(Collections.singletonList(RuleCondition.createCategorical("by_field", "list2"))).build()));

        Detector detector = builder.build();
        assertEquals(new HashSet<>(Arrays.asList("list1", "list2")), detector.extractReferencedFilters());
    }

    public void testInvalid_GivenFieldIsControlField() {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        if (randomBoolean()) {
            detector.setByFieldName(RecordWriter.CONTROL_FIELD_NAME);
        } else if (randomBoolean()) {
            detector.setOverFieldName(RecordWriter.CONTROL_FIELD_NAME);
        } else {
            detector.setPartitionFieldName(RecordWriter.CONTROL_FIELD_NAME);
        }

        ElasticsearchException e = expectThrows(ElasticsearchException.class , detector::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_INVALID_FIELDNAME, RecordWriter.CONTROL_FIELD_NAME,
                RecordWriter.CONTROL_FIELD_NAME), e.getMessage());
    }

    private Detector.Builder createDetector() {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName("by_field");
        detector.setOverFieldName("over_field");
        detector.setPartitionFieldName("partition");
        detector.setUseNull(true);
        Condition condition = new Condition(Operator.GT, "5");
        DetectionRule rule = new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "by_field", "val", condition, null)))
                .setActions(RuleAction.FILTER_RESULTS)
                .setTargetFieldName("over_field")
                .setTargetFieldValue("targetValue")
                .setConditionsConnective(Connective.AND)
                .build();
        detector.setRules(Collections.singletonList(rule));
        return detector;
    }

    @Override
    protected Detector createTestInstance() {
        String function;
        Detector.Builder detector;
        if (randomBoolean()) {
            detector = new Detector.Builder(function = randomFrom(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS), null);
        } else {
            Set<String> functions = new HashSet<>(Detector.FIELD_NAME_FUNCTIONS);
            functions.removeAll(Detector.Builder.FUNCTIONS_WITHOUT_RULE_SUPPORT);
            detector = new Detector.Builder(function = randomFrom(functions), randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            detector.setDetectorDescription(randomAlphaOfLengthBetween(1, 20));
        }
        String fieldName = null;
        if (randomBoolean()) {
            detector.setPartitionFieldName(fieldName = randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean() && Detector.NO_OVER_FIELD_NAME_FUNCTIONS.contains(function) == false) {
            detector.setOverFieldName(fieldName = randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean() && Detector.NO_BY_FIELD_NAME_FUNCTIONS.contains(function) == false) {
            detector.setByFieldName(fieldName = randomAlphaOfLengthBetween(6, 20));
        }
        if (randomBoolean()) {
            detector.setExcludeFrequent(randomFrom(Detector.ExcludeFrequent.values()));
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<DetectionRule> rules = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                // no need for random DetectionRule (it is already tested)
                Condition condition = new Condition(Operator.GT, "5");
                rules.add(new DetectionRule.Builder(
                        Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null)))
                        .setTargetFieldName(fieldName).build());
            }
            detector.setRules(rules);
        }
        if (randomBoolean()) {
            detector.setUseNull(randomBoolean());
        }
        return detector.build();
    }

    @Override
    protected Reader<Detector> instanceReader() {
        return Detector::new;
    }

    @Override
    protected Detector doParseInstance(XContentParser parser) {
        return Detector.CONFIG_PARSER.apply(parser, null).build();
    }

    public void testVerifyFieldNames_givenInvalidChars() {
        Collection<Object[]> testCaseArguments = getCharactersAndValidity();
        for (Object [] args : testCaseArguments) {
            String character = (String) args[0];
            boolean valid = (boolean) args[1];
            Detector.Builder detector = createDetectorWithValidFieldNames();
            verifyFieldName(detector, character, valid);
            detector = createDetectorWithValidFieldNames();
            verifyByFieldName(detector, character, valid);
            detector = createDetectorWithValidFieldNames();
            verifyOverFieldName(detector, character, valid);
            detector = createDetectorWithValidFieldNames();
            verifyPartitionFieldName(detector, character, valid);
        }
    }

    public void testVerifyFunctionForPreSummariedInput() {
        Collection<Object[]> testCaseArguments = getCharactersAndValidity();
        for (Object [] args : testCaseArguments) {
            String character = (String) args[0];
            boolean valid = (boolean) args[1];
            Detector.Builder detector = createDetectorWithValidFieldNames();
            verifyFieldName(detector, character, valid);
            detector = createDetectorWithValidFieldNames();
            verifyByFieldName(new Detector.Builder(detector.build()), character, valid);
            verifyOverFieldName(new Detector.Builder(detector.build()), character, valid);
            verifyByFieldName(new Detector.Builder(detector.build()), character, valid);
            verifyPartitionFieldName(new Detector.Builder(detector.build()), character, valid);
        }
    }

    private static void verifyFieldName(Detector.Builder detector, String character, boolean valid) {
        Detector.Builder updated = createDetectorWithSpecificFieldName(detector.build().getFieldName() + character);
        if (valid == false) {
            expectThrows(ElasticsearchException.class , updated::build);
        }
    }

    private static void verifyByFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setByFieldName(detector.build().getByFieldName() + character);
        if (valid == false) {
            expectThrows(ElasticsearchException.class , detector::build);
        }
    }

    private static void verifyOverFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setOverFieldName(detector.build().getOverFieldName() + character);
        if (valid == false) {
            expectThrows(ElasticsearchException.class , detector::build);
        }
    }

    private static void verifyPartitionFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setPartitionFieldName(detector.build().getPartitionFieldName() + character);
        if (valid == false) {
            expectThrows(ElasticsearchException.class , detector::build);
        }
    }

    private static Detector.Builder createDetectorWithValidFieldNames() {
        Detector.Builder d = new Detector.Builder("metric", "field");
        d.setByFieldName("by_field");
        d.setOverFieldName("over_field");
        d.setPartitionFieldName("partition");
        return d;
    }

    private static Detector.Builder createDetectorWithSpecificFieldName(String fieldName) {
        Detector.Builder d = new Detector.Builder("metric", fieldName);
        d.setByFieldName("by_field");
        d.setOverFieldName("over_field");
        d.setPartitionFieldName("partition");
        return d;
    }

    private static Collection<Object[]> getCharactersAndValidity() {
        return Arrays.asList(new Object[][]{
                // char, isValid?
                {"a", true},
                {"[", true},
                {"]", true},
                {"(", true},
                {")", true},
                {"=", true},
                {"-", true},
                {" ", true},
                {"\"", false},
                {"\\", false},
                {"\t", false},
                {"\n", false},
        });
    }

    public void testVerify() throws Exception {
        // if nothing else is set the count functions (excluding distinct count)
        // are the only allowable functions
        new Detector.Builder(Detector.COUNT, null).build();

        Set<String> difference = new HashSet<String>(Detector.ANALYSIS_FUNCTIONS);
        difference.remove(Detector.COUNT);
        difference.remove(Detector.HIGH_COUNT);
        difference.remove(Detector.LOW_COUNT);
        difference.remove(Detector.NON_ZERO_COUNT);
        difference.remove(Detector.NZC);
        difference.remove(Detector.LOW_NON_ZERO_COUNT);
        difference.remove(Detector.LOW_NZC);
        difference.remove(Detector.HIGH_NON_ZERO_COUNT);
        difference.remove(Detector.HIGH_NZC);
        difference.remove(Detector.TIME_OF_DAY);
        difference.remove(Detector.TIME_OF_WEEK);
        for (String f : difference) {
            try {
                new Detector.Builder(f, null).build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }

        // certain fields aren't allowed with certain functions
        // first do the over field
        for (String f : new String[]{Detector.NON_ZERO_COUNT, Detector.NZC,
                Detector.LOW_NON_ZERO_COUNT, Detector.LOW_NZC, Detector.HIGH_NON_ZERO_COUNT,
                Detector.HIGH_NZC}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            try {
                builder.build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }

        // these functions cannot have just an over field
        difference = new HashSet<>(Detector.ANALYSIS_FUNCTIONS);
        difference.remove(Detector.COUNT);
        difference.remove(Detector.HIGH_COUNT);
        difference.remove(Detector.LOW_COUNT);
        difference.remove(Detector.TIME_OF_DAY);
        difference.remove(Detector.TIME_OF_WEEK);
        for (String f : difference) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            try {
                builder.build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }

        // these functions can have just an over field
        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            builder.build();
        }

        for (String f : new String[]{Detector.RARE, Detector.FREQ_RARE}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            builder.setByFieldName("by_field");
            builder.build();
        }

        // some functions require a fieldname
        for (String f : new String[]{Detector.DISTINCT_COUNT, Detector.DC,
                Detector.HIGH_DISTINCT_COUNT, Detector.HIGH_DC, Detector.LOW_DISTINCT_COUNT, Detector.LOW_DC,
                Detector.INFO_CONTENT, Detector.LOW_INFO_CONTENT, Detector.HIGH_INFO_CONTENT,
                Detector.METRIC, Detector.MEAN, Detector.HIGH_MEAN, Detector.LOW_MEAN, Detector.AVG,
                Detector.HIGH_AVG, Detector.LOW_AVG, Detector.MAX, Detector.MIN, Detector.SUM,
                Detector.LOW_SUM, Detector.HIGH_SUM, Detector.NON_NULL_SUM,
                Detector.LOW_NON_NULL_SUM, Detector.HIGH_NON_NULL_SUM, Detector.POPULATION_VARIANCE,
                Detector.LOW_POPULATION_VARIANCE, Detector.HIGH_POPULATION_VARIANCE}) {
            Detector.Builder builder = new Detector.Builder(f, "f");
            builder.setOverFieldName("over_field");
            builder.build();
        }

        // these functions cannot have a field name
        difference = new HashSet<>(Detector.ANALYSIS_FUNCTIONS);
        difference.remove(Detector.METRIC);
        difference.remove(Detector.MEAN);
        difference.remove(Detector.LOW_MEAN);
        difference.remove(Detector.HIGH_MEAN);
        difference.remove(Detector.AVG);
        difference.remove(Detector.LOW_AVG);
        difference.remove(Detector.HIGH_AVG);
        difference.remove(Detector.MEDIAN);
        difference.remove(Detector.LOW_MEDIAN);
        difference.remove(Detector.HIGH_MEDIAN);
        difference.remove(Detector.MIN);
        difference.remove(Detector.MAX);
        difference.remove(Detector.SUM);
        difference.remove(Detector.LOW_SUM);
        difference.remove(Detector.HIGH_SUM);
        difference.remove(Detector.NON_NULL_SUM);
        difference.remove(Detector.LOW_NON_NULL_SUM);
        difference.remove(Detector.HIGH_NON_NULL_SUM);
        difference.remove(Detector.POPULATION_VARIANCE);
        difference.remove(Detector.LOW_POPULATION_VARIANCE);
        difference.remove(Detector.HIGH_POPULATION_VARIANCE);
        difference.remove(Detector.DISTINCT_COUNT);
        difference.remove(Detector.HIGH_DISTINCT_COUNT);
        difference.remove(Detector.LOW_DISTINCT_COUNT);
        difference.remove(Detector.DC);
        difference.remove(Detector.LOW_DC);
        difference.remove(Detector.HIGH_DC);
        difference.remove(Detector.INFO_CONTENT);
        difference.remove(Detector.LOW_INFO_CONTENT);
        difference.remove(Detector.HIGH_INFO_CONTENT);
        difference.remove(Detector.LAT_LONG);
        for (String f : difference) {
            Detector.Builder builder = new Detector.Builder(f, "f");
            builder.setOverFieldName("over_field");
            try {
                builder.build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }

        // these can have a by field
        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.RARE,
                Detector.NON_ZERO_COUNT, Detector.NZC}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setByFieldName("b");
            builder.build();
        }

        Detector.Builder builder = new Detector.Builder(Detector.FREQ_RARE, null);
        builder.setOverFieldName("over_field");
        builder.setByFieldName("b");
        builder.build();
        builder = new Detector.Builder(Detector.FREQ_RARE, null);
        builder.setOverFieldName("over_field");
        builder.setByFieldName("b");
        builder.build();

        // some functions require a fieldname
        int testedFunctionsCount = 0;
        for (String f : Detector.FIELD_NAME_FUNCTIONS) {
            testedFunctionsCount++;
            builder = new Detector.Builder(f, "f");
            builder.setByFieldName("b");
            builder.build();
        }
        Assert.assertEquals(Detector.FIELD_NAME_FUNCTIONS.size(), testedFunctionsCount);

        // these functions don't work with fieldname
        testedFunctionsCount = 0;
        for (String f : Detector.COUNT_WITHOUT_FIELD_FUNCTIONS) {
            testedFunctionsCount++;
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("b");
                builder.build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }
        Assert.assertEquals(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS.size(), testedFunctionsCount);

        builder = new Detector.Builder(Detector.FREQ_RARE, "field");
        builder.setByFieldName("b");
        builder.setOverFieldName("over_field");
        try {
            builder.build();
            Assert.fail("ElasticsearchException not thrown when expected");
        } catch (ElasticsearchException e) {
        }

        for (String f : new String[]{Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.NON_ZERO_COUNT, Detector.NZC}) {
            builder = new Detector.Builder(f, null);
            builder.setByFieldName("by_field");
            builder.build();
        }

        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            builder.build();
        }

        for (String f : new String[]{Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            builder = new Detector.Builder(f, null);
            builder.setByFieldName("by_field");
            builder.setOverFieldName("over_field");
            builder.build();
        }

        for (String f : new String[]{Detector.NON_ZERO_COUNT, Detector.NZC}) {
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("by_field");
                builder.setOverFieldName("over_field");
                builder.build();
                Assert.fail("ElasticsearchException not thrown when expected");
            } catch (ElasticsearchException e) {
            }
        }
    }

    public void testVerify_GivenInvalidRuleTargetFieldName() {
        Detector.Builder detector = new Detector.Builder("mean", "metricVale");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        RuleCondition ruleCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "metricName", "metricVale", new Condition(Operator.LT, "5"), null);
        DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(ruleCondition)).setTargetFieldName("instancE").build();
        detector.setRules(Collections.singletonList(rule));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_INVALID_TARGET_FIELD_NAME,
                "[metricName, instance]", "instancE"),
                e.getMessage());
    }

    public void testVerify_GivenValidRule() {
        Detector.Builder detector = new Detector.Builder("mean", "metricVale");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        RuleCondition ruleCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "metricName", "CPU", new Condition(Operator.LT, "5"), null);
        DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(ruleCondition)).setTargetFieldName("instance").build();
        detector.setRules(Collections.singletonList(rule));
        detector.build();
    }

    public void testVerify_GivenSameByAndPartition() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("x");
        detector.setPartitionFieldName("x");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("partition_field_name and by_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenSameByAndOver() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("x");
        detector.setOverFieldName("x");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("by_field_name and over_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenSameOverAndPartition() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("x");
        detector.setPartitionFieldName("x");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("partition_field_name and over_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenByIsCount() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("count");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'count' is not a permitted value for by_field_name", e.getMessage());
    }

    public void testVerify_GivenOverIsCount() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("count");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'count' is not a permitted value for over_field_name", e.getMessage());
    }

    public void testVerify_GivenByIsBy() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("by");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'by' is not a permitted value for by_field_name", e.getMessage());
    }

    public void testVerify_GivenOverIsBy() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("by");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'by' is not a permitted value for over_field_name", e.getMessage());
    }

    public void testVerify_GivenByIsOver() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("over");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'over' is not a permitted value for by_field_name", e.getMessage());
    }

    public void testVerify_GivenOverIsOver() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("over");
        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals("'over' is not a permitted value for over_field_name", e.getMessage());
    }

    public void testExcludeFrequentForString() {
        assertEquals(Detector.ExcludeFrequent.ALL, Detector.ExcludeFrequent.forString("all"));
        assertEquals(Detector.ExcludeFrequent.ALL, Detector.ExcludeFrequent.forString("ALL"));
        assertEquals(Detector.ExcludeFrequent.NONE, Detector.ExcludeFrequent.forString("none"));
        assertEquals(Detector.ExcludeFrequent.NONE, Detector.ExcludeFrequent.forString("NONE"));
        assertEquals(Detector.ExcludeFrequent.BY, Detector.ExcludeFrequent.forString("by"));
        assertEquals(Detector.ExcludeFrequent.BY, Detector.ExcludeFrequent.forString("BY"));
        assertEquals(Detector.ExcludeFrequent.OVER, Detector.ExcludeFrequent.forString("over"));
        assertEquals(Detector.ExcludeFrequent.OVER, Detector.ExcludeFrequent.forString("OVER"));
    }
}
