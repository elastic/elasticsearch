/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.process.writer.RecordWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

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
        detector2 = builder.build();
        assertFalse(detector1.equals(detector2));
    }

    public void testExtractAnalysisFields() {
        DetectionRule rule = new DetectionRule.Builder(
                Collections.singletonList(new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 5)))
                .setActions(RuleAction.SKIP_RESULT)
                .build();
        Detector.Builder builder = createDetector();
        builder.setRules(Collections.singletonList(rule));
        Detector detector = builder.build();
        assertEquals(Arrays.asList("by_field", "over_field", "partition"), detector.extractAnalysisFields());

        builder.setPartitionFieldName(null);

        detector = builder.build();

        assertEquals(Arrays.asList("by_field", "over_field"), detector.extractAnalysisFields());

        builder = new Detector.Builder(detector);
        builder.setByFieldName(null);

        detector = builder.build();

        assertEquals(Collections.singletonList("over_field"), detector.extractAnalysisFields());

        builder = new Detector.Builder(detector);
        builder.setOverFieldName(null);

        detector = builder.build();

        assertTrue(detector.extractAnalysisFields().isEmpty());
    }

    public void testExtractReferencedLists() {
        Detector.Builder builder = createDetector();
        builder.setRules(Arrays.asList(
                new DetectionRule.Builder(RuleScope.builder().exclude("by_field", "list1")).build(),
                new DetectionRule.Builder(RuleScope.builder().exclude("by_field", "list2")).build()));

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
        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().exclude("partition", "partition_filter"))
                .setActions(RuleAction.SKIP_RESULT)
                .build();
        detector.setRules(Collections.singletonList(rule));
        return detector;
    }

    @Override
    protected Detector createTestInstance() {
        DetectorFunction function;
        Detector.Builder detector;
        if (randomBoolean()) {
            detector = new Detector.Builder(function = randomFrom(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS), null);
        } else {
            EnumSet<DetectorFunction> functions = EnumSet.copyOf(Detector.FIELD_NAME_FUNCTIONS);
            detector = new Detector.Builder(function = randomFrom(functions), randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            detector.setDetectorDescription(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            detector.setPartitionFieldName(randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean() && Detector.NO_OVER_FIELD_NAME_FUNCTIONS.contains(function) == false) {
            detector.setOverFieldName(randomAlphaOfLengthBetween(6, 20));
        } else if (randomBoolean()) {
            detector.setByFieldName(randomAlphaOfLengthBetween(6, 20));
        }
        if (randomBoolean()) {
            detector.setExcludeFrequent(randomFrom(Detector.ExcludeFrequent.values()));
        }
        if (Detector.FUNCTIONS_WITHOUT_RULE_CONDITION_SUPPORT.contains(function) == false && randomBoolean()) {
            int size = randomInt(10);
            List<DetectionRule> rules = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                // no need for random DetectionRule (it is already tested)
                rules.add(new DetectionRule.Builder(Collections.singletonList(RuleConditionTests.createRandom())).build());
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
        return Detector.STRICT_PARSER.apply(parser, null).build();
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

    public void testVerify_GivenFunctionOnly() {
        // if nothing else is set the count functions (excluding distinct count)
        // are the only allowable functions
        new Detector.Builder(DetectorFunction.COUNT, null).build();

        EnumSet<DetectorFunction> difference = EnumSet.allOf(DetectorFunction.class);
        difference.remove(DetectorFunction.COUNT);
        difference.remove(DetectorFunction.HIGH_COUNT);
        difference.remove(DetectorFunction.LOW_COUNT);
        difference.remove(DetectorFunction.NON_ZERO_COUNT);
        difference.remove(DetectorFunction.LOW_NON_ZERO_COUNT);
        difference.remove(DetectorFunction.HIGH_NON_ZERO_COUNT);
        difference.remove(DetectorFunction.TIME_OF_DAY);
        difference.remove(DetectorFunction.TIME_OF_WEEK);
        for (DetectorFunction f : difference) {
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                    () -> new Detector.Builder(f, null).build());
            assertThat(e.getMessage(), equalTo("Unless a count or temporal function is used one of field_name," +
                    " by_field_name or over_field_name must be set"));
        }
    }

    public void testVerify_GivenFunctionsNotSupportingOverField() {
        EnumSet<DetectorFunction> noOverFieldFunctions = EnumSet.of(
                DetectorFunction.NON_ZERO_COUNT,
                DetectorFunction.LOW_NON_ZERO_COUNT,
                DetectorFunction.HIGH_NON_ZERO_COUNT
        );
        for (DetectorFunction f: noOverFieldFunctions) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder.build());
            assertThat(e.getMessage(), equalTo("over_field_name cannot be used with function '" + f + "'"));
        }
    }

    public void testVerify_GivenFunctionsCannotHaveJustOverField() {
        EnumSet<DetectorFunction> difference = EnumSet.allOf(DetectorFunction.class);
        difference.remove(DetectorFunction.COUNT);
        difference.remove(DetectorFunction.LOW_COUNT);
        difference.remove(DetectorFunction.HIGH_COUNT);
        difference.remove(DetectorFunction.TIME_OF_DAY);
        difference.remove(DetectorFunction.TIME_OF_WEEK);
        for (DetectorFunction f: difference) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            expectThrows(ElasticsearchStatusException.class, () -> builder.build());
        }
    }

    public void testVerify_GivenFunctionsCanHaveJustOverField() {
        EnumSet<DetectorFunction> noOverFieldFunctions = EnumSet.of(
                DetectorFunction.COUNT,
                DetectorFunction.LOW_COUNT,
                DetectorFunction.HIGH_COUNT
        );
        for (DetectorFunction f: noOverFieldFunctions) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            builder.build();
        }
    }

    public void testVerify_GivenFunctionsCannotHaveFieldName() {
        for (DetectorFunction f : Detector.COUNT_WITHOUT_FIELD_FUNCTIONS) {
            Detector.Builder builder = new Detector.Builder(f, "field");
            builder.setByFieldName("b");
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder.build());
            assertThat(e.getMessage(), equalTo("field_name cannot be used with function '" + f + "'"));
        }

        // Nor rare
        {
            Detector.Builder builder = new Detector.Builder(DetectorFunction.RARE, "field");
            builder.setByFieldName("b");
            builder.setOverFieldName("over_field");
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder.build());
            assertThat(e.getMessage(), equalTo("field_name cannot be used with function 'rare'"));
        }

        // Nor freq_rare
        {
            Detector.Builder builder = new Detector.Builder(DetectorFunction.FREQ_RARE, "field");
            builder.setByFieldName("b");
            builder.setOverFieldName("over_field");
            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder.build());
            assertThat(e.getMessage(), equalTo("field_name cannot be used with function 'freq_rare'"));
        }
    }

    public void testVerify_GivenFunctionsRequiringFieldName() {
        // some functions require a fieldname
        for (DetectorFunction f : Detector.FIELD_NAME_FUNCTIONS) {
            Detector.Builder builder = new Detector.Builder(f, "f");
            builder.build();
        }
    }

    public void testVerify_GivenFieldNameFunctionsAndOverField() {
        // some functions require a fieldname
        for (DetectorFunction f : Detector.FIELD_NAME_FUNCTIONS) {
            Detector.Builder builder = new Detector.Builder(f, "f");
            builder.setOverFieldName("some_over_field");
            builder.build();
        }
    }

    public void testVerify_GivenFieldNameFunctionsAndByField() {
        // some functions require a fieldname
        for (DetectorFunction f : Detector.FIELD_NAME_FUNCTIONS) {
            Detector.Builder builder = new Detector.Builder(f, "f");
            builder.setByFieldName("some_by_field");
            builder.build();
        }
    }

    public void testVerify_GivenCountFunctionsWithByField() {
        // some functions require a fieldname
        for (DetectorFunction f : Detector.COUNT_WITHOUT_FIELD_FUNCTIONS) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setByFieldName("some_by_field");
            builder.build();
        }
    }

    public void testVerify_GivenCountFunctionsWithOverField() {
        EnumSet<DetectorFunction> functions = EnumSet.copyOf(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS);
        functions.removeAll(Detector.NO_OVER_FIELD_NAME_FUNCTIONS);
        for (DetectorFunction f : functions) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("some_over_field");
            builder.build();
        }
    }

    public void testVerify_GivenCountFunctionsWithByAndOverFields() {
        EnumSet<DetectorFunction> functions = EnumSet.copyOf(Detector.COUNT_WITHOUT_FIELD_FUNCTIONS);
        functions.removeAll(Detector.NO_OVER_FIELD_NAME_FUNCTIONS);
        for (DetectorFunction f : functions) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setByFieldName("some_over_field");
            builder.setOverFieldName("some_by_field");
            builder.build();
        }
    }

    public void testVerify_GivenRareAndFreqRareWithByAndOverFields() {
        for (DetectorFunction f : EnumSet.of(DetectorFunction.RARE, DetectorFunction.FREQ_RARE)) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setOverFieldName("over_field");
            builder.setByFieldName("by_field");
            builder.build();
        }
    }

    public void testVerify_GivenFunctionsThatCanHaveByField() {
        for (DetectorFunction f : EnumSet.of(DetectorFunction.COUNT, DetectorFunction.HIGH_COUNT, DetectorFunction.LOW_COUNT,
                DetectorFunction.RARE, DetectorFunction.NON_ZERO_COUNT, DetectorFunction.LOW_NON_ZERO_COUNT,
                DetectorFunction.HIGH_NON_ZERO_COUNT)) {
            Detector.Builder builder = new Detector.Builder(f, null);
            builder.setByFieldName("b");
            builder.build();
        }
    }

    public void testVerify_GivenAllPartitioningFieldsAreScoped() {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setPartitionFieldName("my_partition");
        detector.setOverFieldName("my_over");
        detector.setByFieldName("my_by");

        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder()
                .exclude("my_partition", "my_filter_id")
                .exclude("my_over", "my_filter_id")
                .exclude("my_by", "my_filter_id"))
                .build();
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

    public void testVerify_GivenRulesAndFunctionIsLatLong() {
        Detector.Builder detector = new Detector.Builder("lat_long", "geo");
        detector.setRules(Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 42.0))).build()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertThat(e.getMessage(), equalTo("Invalid detector rule: function lat_long only supports conditions that apply to time"));
    }

    public void testVerify_GivenRulesAndFunctionIsMetric() {
        Detector.Builder detector = new Detector.Builder("metric", "some_metric");
        detector.setRules(Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.TYPICAL, Operator.GT, 42.0))).build()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertThat(e.getMessage(), equalTo("Invalid detector rule: function metric only supports conditions that apply to time"));
    }

    public void testVerify_GivenRulesAndFunctionIsRare() {
        Detector.Builder detector = new Detector.Builder("rare", null);
        detector.setByFieldName("some_field");
        detector.setRules(Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.DIFF_FROM_TYPICAL, Operator.GT, 42.0))).build()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertThat(e.getMessage(), equalTo("Invalid detector rule: function rare only supports conditions that apply to time"));
    }

    public void testVerify_GivenRulesAndFunctionIsFreqRare() {
        Detector.Builder detector = new Detector.Builder("freq_rare", null);
        detector.setByFieldName("some_field");
        detector.setOverFieldName("some_field2");
        detector.setRules(Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.ACTUAL, Operator.GT, 42.0))).build()));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertThat(e.getMessage(), equalTo("Invalid detector rule: function freq_rare only supports conditions that apply to time"));
    }

    public void testVerify_GivenTimeConditionRuleAndFunctionIsLatLong() {
        Detector.Builder detector = new Detector.Builder("lat_long", "geo");
        detector.setRules(Collections.singletonList(new DetectionRule.Builder(Collections.singletonList(
                new RuleCondition(RuleCondition.AppliesTo.TIME, Operator.GT, 42.0))).build()));
        detector.build();
    }

    public void testVerify_GivenScopeRuleOnInvalidField() {
        Detector.Builder detector = new Detector.Builder("mean", "my_metric");
        detector.setPartitionFieldName("my_partition");
        detector.setOverFieldName("my_over");
        detector.setByFieldName("my_by");

        DetectionRule rule = new DetectionRule.Builder(RuleScope.builder().exclude("my_metric", "my_filter_id")).build();
        detector.setRules(Collections.singletonList(rule));

        ElasticsearchException e = ESTestCase.expectThrows(ElasticsearchException.class, detector::build);

        assertEquals(Messages.getMessage(Messages.JOB_CONFIG_DETECTION_RULE_SCOPE_HAS_INVALID_FIELD,
                "my_metric", "[my_by, my_over, my_partition]"), e.getMessage());
    }

    public void testVerify_GivenValidRule() {
        Detector.Builder detector = new Detector.Builder("mean", "metricVale");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        DetectionRule rule = new DetectionRule.Builder(Collections.singletonList(RuleConditionTests.createRandom()))
                .setScope(RuleScope.builder()
                        .include("metricName", "f1")
                        .exclude("instance", "f2")
                        .build())
                .build();
        detector.setRules(Collections.singletonList(rule));
        detector.build();
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
