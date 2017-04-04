/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;
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
        builder.setByFieldName("by");
        builder.setOverFieldName("over");
        builder.setPartitionFieldName("partition");
        builder.setUseNull(false);
        Detector detector1 = builder.build();

        builder = new Detector.Builder("mean", "field");
        builder.setByFieldName("by");
        builder.setOverFieldName("over");
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
        DetectionRule rule = new DetectionRule("over", "targetValue", Connective.AND,
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "by2", "val", condition, null)));
        builder.setDetectorRules(Collections.singletonList(rule));
        detector2 = builder.build();
        assertFalse(detector1.equals(detector2));
    }

    public void testEquals_GivenDifferentRules() {
        Detector detector1 = createDetector().build();
        Detector.Builder builder = new Detector.Builder(detector1);
        DetectionRule rule = new DetectionRule(builder.getDetectorRules().get(0).getTargetFieldName(),
                builder.getDetectorRules().get(0).getTargetFieldValue(), Connective.OR,
                builder.getDetectorRules().get(0).getRuleConditions());
        builder.getDetectorRules().set(0, rule);
        Detector detector2 = builder.build();

        assertFalse(detector1.equals(detector2));
        assertFalse(detector2.equals(detector1));
    }

    public void testExtractAnalysisFields() {
        Detector detector = createDetector().build();
        assertEquals(Arrays.asList("by", "over", "partition"), detector.extractAnalysisFields());
        Detector.Builder builder = new Detector.Builder(detector);
        builder.setPartitionFieldName(null);
        detector = builder.build();
        assertEquals(Arrays.asList("by", "over"), detector.extractAnalysisFields());
        builder = new Detector.Builder(detector);
        Condition condition = new Condition(Operator.GT, "5");
        DetectionRule rule = new DetectionRule("over", "targetValue", Connective.AND,
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null)));
        builder.setDetectorRules(Collections.singletonList(rule));
        builder.setByFieldName(null);
        detector = builder.build();
        assertEquals(Arrays.asList("over"), detector.extractAnalysisFields());
        builder = new Detector.Builder(detector);
        rule = new DetectionRule(null, null, Connective.AND,
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null)));
        builder.setDetectorRules(Collections.singletonList(rule));
        builder.setOverFieldName(null);
        detector = builder.build();
        assertTrue(detector.extractAnalysisFields().isEmpty());
    }

    public void testExtractReferencedLists() {
        Detector.Builder builder = createDetector();
        builder.setDetectorRules(Arrays.asList(
                new DetectionRule(null, null, Connective.OR, Arrays.asList(RuleCondition.createCategorical("by", "list1"))),
                new DetectionRule(null, null, Connective.OR, Arrays.asList(RuleCondition.createCategorical("by", "list2")))));

        Detector detector = builder.build();
        assertEquals(new HashSet<>(Arrays.asList("list1", "list2")), detector.extractReferencedFilters());
    }

    private Detector.Builder createDetector() {
        Detector.Builder detector = new Detector.Builder("mean", "field");
        detector.setByFieldName("by");
        detector.setOverFieldName("over");
        detector.setPartitionFieldName("partition");
        detector.setUseNull(true);
        Condition condition = new Condition(Operator.GT, "5");
        DetectionRule rule = new DetectionRule("over", "targetValue", Connective.AND,
                Collections.singletonList(new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "by", "val", condition, null)));
        detector.setDetectorRules(Arrays.asList(rule));
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
            detector.setPartitionFieldName(fieldName = randomAlphaOfLengthBetween(1, 20));
        } else if (randomBoolean() && Detector.NO_OVER_FIELD_NAME_FUNCTIONS.contains(function) == false) {
            detector.setOverFieldName(fieldName = randomAlphaOfLengthBetween(1, 20));
        } else if (randomBoolean() && Detector.NO_BY_FIELD_NAME_FUNCTIONS.contains(function) == false) {
            detector.setByFieldName(fieldName = randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            detector.setExcludeFrequent(randomFrom(Detector.ExcludeFrequent.values()));
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<DetectionRule> detectorRules = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                // no need for random DetectionRule (it is already tested)
                Condition condition = new Condition(Operator.GT, "5");
                detectorRules.add(new DetectionRule(fieldName, null, Connective.OR, Collections.singletonList(
                        new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, null, null, condition, null))
                ));
            }
            detector.setDetectorRules(detectorRules);
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
    protected Detector parseInstance(XContentParser parser) {
        return Detector.PARSER.apply(parser, null).build();
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
            verifyFieldNameGivenPresummarised(detector, character, valid);
            detector = createDetectorWithValidFieldNames();
            verifyByFieldNameGivenPresummarised(new Detector.Builder(detector.build()), character, valid);
            verifyOverFieldNameGivenPresummarised(new Detector.Builder(detector.build()), character, valid);
            verifyByFieldNameGivenPresummarised(new Detector.Builder(detector.build()), character, valid);
            verifyPartitionFieldNameGivenPresummarised(new Detector.Builder(detector.build()), character, valid);
        }
    }

    private static void verifyFieldName(Detector.Builder detector, String character, boolean valid) {
        Detector.Builder updated = createDetectorWithSpecificFieldName(detector.build().getFieldName() + character);
        if (valid == false) {
            expectThrows(IllegalArgumentException.class , () -> updated.build());
        }
    }

    private static void verifyByFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setByFieldName(detector.build().getByFieldName() + character);
        if (valid == false) {
            expectThrows(IllegalArgumentException.class , () -> detector.build());
        }
    }

    private static void verifyOverFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setOverFieldName(detector.build().getOverFieldName() + character);
        if (valid == false) {
            expectThrows(IllegalArgumentException.class , () -> detector.build());
        }
    }

    private static void verifyPartitionFieldName(Detector.Builder detector, String character, boolean valid) {
        detector.setPartitionFieldName(detector.build().getPartitionFieldName() + character);
        if (valid == false) {
            expectThrows(IllegalArgumentException.class , () -> detector.build());
        }
    }

    private static void verifyFieldNameGivenPresummarised(Detector.Builder detector, String character, boolean valid) {
        Detector.Builder updated = createDetectorWithSpecificFieldName(detector.build().getFieldName() + character);
        expectThrows(IllegalArgumentException.class , () -> updated.build(true));
    }

    private static void verifyByFieldNameGivenPresummarised(Detector.Builder detector, String character, boolean valid) {
        detector.setByFieldName(detector.build().getByFieldName() + character);
        expectThrows(IllegalArgumentException.class , () -> detector.build(true));
    }

    private static void verifyOverFieldNameGivenPresummarised(Detector.Builder detector, String character, boolean valid) {
        detector.setOverFieldName(detector.build().getOverFieldName() + character);
        expectThrows(IllegalArgumentException.class , () -> detector.build(true));
    }

    private static void verifyPartitionFieldNameGivenPresummarised(Detector.Builder detector, String character, boolean valid) {
        detector.setPartitionFieldName(detector.build().getPartitionFieldName() + character);
        expectThrows(IllegalArgumentException.class , () -> detector.build(true));
    }

    private static Detector.Builder createDetectorWithValidFieldNames() {
        Detector.Builder d = new Detector.Builder("metric", "field");
        d.setByFieldName("by");
        d.setOverFieldName("over");
        d.setPartitionFieldName("partition");
        return d;
    }

    private static Detector.Builder createDetectorWithSpecificFieldName(String fieldName) {
        Detector.Builder d = new Detector.Builder("metric", fieldName);
        d.setByFieldName("by");
        d.setOverFieldName("over");
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
        new Detector.Builder(Detector.COUNT, null).build(true);

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
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                new Detector.Builder(f, null).build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }

        // certain fields aren't allowed with certain functions
        // first do the over field
        for (String f : new String[]{Detector.NON_ZERO_COUNT, Detector.NZC,
                Detector.LOW_NON_ZERO_COUNT, Detector.LOW_NZC, Detector.HIGH_NON_ZERO_COUNT,
                Detector.HIGH_NZC}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over");
            try {
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
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
            builder.setOverFieldName("over");
            try {
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }

        // these functions can have just an over field
        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over");
            builder.build();
            builder.build(true);
        }

        for (String f : new String[]{Detector.RARE, Detector.FREQ_RARE}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over");
            builder.setByFieldName("by");
            builder.build();
            builder.build(true);
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
            builder.setOverFieldName("over");
            builder.build();
            try {
                builder.build(true);
                Assert.assertFalse(Detector.METRIC.equals(f));
            } catch (IllegalArgumentException e) {
                // "metric" is not allowed as the function for pre-summarised input
                Assert.assertEquals(Detector.METRIC, f);
            }
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
            builder.setOverFieldName("over");
            try {
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }

        // these can have a by field
        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.RARE,
                Detector.NON_ZERO_COUNT, Detector.NZC}) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setByFieldName("b");
            builder.build();
            builder.build(true);
        }

        Detector.Builder builder = new Detector.Builder(Detector.FREQ_RARE, null);
        builder.setOverFieldName("over");
        builder.setByFieldName("b");
        builder.build();
        builder.build(true);
        builder = new Detector.Builder(Detector.FREQ_RARE, null);
        builder.setOverFieldName("over");
        builder.setByFieldName("b");
        builder.build();

        // some functions require a fieldname
        for (String f : new String[]{Detector.METRIC, Detector.MEAN, Detector.HIGH_MEAN,
                Detector.LOW_MEAN, Detector.AVG, Detector.HIGH_AVG, Detector.LOW_AVG,
                Detector.MEDIAN, Detector.MAX, Detector.MIN, Detector.SUM, Detector.LOW_SUM,
                Detector.HIGH_SUM, Detector.NON_NULL_SUM, Detector.LOW_NON_NULL_SUM,
                Detector.HIGH_NON_NULL_SUM, Detector.POPULATION_VARIANCE,
                Detector.LOW_POPULATION_VARIANCE, Detector.HIGH_POPULATION_VARIANCE,
                Detector.DISTINCT_COUNT, Detector.DC,
                Detector.HIGH_DISTINCT_COUNT, Detector.HIGH_DC, Detector.LOW_DISTINCT_COUNT,
                Detector.LOW_DC, Detector.INFO_CONTENT, Detector.LOW_INFO_CONTENT,
                Detector.HIGH_INFO_CONTENT, Detector.LAT_LONG}) {
            builder = new Detector.Builder(f, "f");
            builder.setByFieldName("b");
            builder.build();
            try {
                builder.build(true);
                Assert.assertFalse(Detector.METRIC.equals(f));
            } catch (IllegalArgumentException e) {
                // "metric" is not allowed as the function for pre-summarised input
                Assert.assertEquals(Detector.METRIC, f);
            }
        }


        // these functions don't work with fieldname
        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.NON_ZERO_COUNT, Detector.NZC,
                Detector.RARE, Detector.FREQ_RARE, Detector.TIME_OF_DAY,
                Detector.TIME_OF_WEEK}) {
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("b");
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("b");
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }


        for (String f : new String[]{Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.NON_ZERO_COUNT, Detector.NZC,
                Detector.RARE, Detector.FREQ_RARE, Detector.TIME_OF_DAY,
                Detector.TIME_OF_WEEK}) {
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("b");
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("b");
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }

        builder = new Detector.Builder(Detector.FREQ_RARE, "field");
        builder.setByFieldName("b");
        builder.setOverFieldName("over");
        try {
            builder.build();
            Assert.fail("IllegalArgumentException not thrown when expected");
        } catch (IllegalArgumentException e) {
        }
        try {
            builder.build(true);
            Assert.fail("IllegalArgumentException not thrown when expected");
        } catch (IllegalArgumentException e) {
        }


        for (String f : new String[]{Detector.HIGH_COUNT,
                Detector.LOW_COUNT, Detector.NON_ZERO_COUNT, Detector.NZC}) {
            builder = new Detector.Builder(f, null);
            builder.setByFieldName("by");
            builder.build();
            builder.build(true);
        }

        for (String f : new String[]{Detector.COUNT, Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over");
            builder.build();
            builder.build(true);
        }

        for (String f : new String[]{Detector.HIGH_COUNT,
                Detector.LOW_COUNT}) {
            builder = new Detector.Builder(f, null);
            builder.setByFieldName("by");
            builder.setOverFieldName("over");
            builder.build();
            builder.build(true);
        }

        for (String f : new String[]{Detector.NON_ZERO_COUNT, Detector.NZC}) {
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("by");
                builder.setOverFieldName("over");
                builder.build();
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
            try {
                builder = new Detector.Builder(f, "field");
                builder.setByFieldName("by");
                builder.setOverFieldName("over");
                builder.build(true);
                Assert.fail("IllegalArgumentException not thrown when expected");
            } catch (IllegalArgumentException e) {
            }
        }
    }

    public void testVerify_GivenInvalidDetectionRuleTargetFieldName() {
        Detector.Builder detector = new Detector.Builder("mean", "metricVale");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        RuleCondition ruleCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "metricName", "metricVale", new Condition(Operator.LT, "5"), null);
        DetectionRule rule = new DetectionRule("instancE", null, Connective.OR, Arrays.asList(ruleCondition));
        detector.setDetectorRules(Arrays.asList(rule));

        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_INVALID_TARGET_FIELD_NAME,
                "[metricName, instance]", "instancE"),
                e.getMessage());
    }

    public void testVerify_GivenValidDetectionRule() {
        Detector.Builder detector = new Detector.Builder("mean", "metricVale");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        RuleCondition ruleCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "metricName", "CPU", new Condition(Operator.LT, "5"), null);
        DetectionRule rule = new DetectionRule("instance", null, Connective.OR, Arrays.asList(ruleCondition));
        detector.setDetectorRules(Arrays.asList(rule));
        detector.build();
    }

    public void testVerify_GivenSameByAndPartition() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("x");
        detector.setPartitionFieldName("x");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals("partition_field_name and by_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenSameByAndOver() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("x");
        detector.setOverFieldName("x");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals("by_field_name and over_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenSameOverAndPartition() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("x");
        detector.setPartitionFieldName("x");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals("partition_field_name and over_field_name cannot be the same: 'x'", e.getMessage());
    }

    public void testVerify_GivenByIsCount() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setByFieldName("count");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals("'count' is not a permitted value for by_field_name", e.getMessage());
    }

    public void testVerify_GivenOverIsCount() {
        Detector.Builder detector = new Detector.Builder("count", "");
        detector.setOverFieldName("count");
        IllegalArgumentException e = ESTestCase.expectThrows(IllegalArgumentException.class, detector::build);

        assertEquals("'count' is not a permitted value for over_field_name", e.getMessage());
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
