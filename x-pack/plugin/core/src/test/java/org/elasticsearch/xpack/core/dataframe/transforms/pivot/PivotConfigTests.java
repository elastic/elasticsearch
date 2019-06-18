/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class PivotConfigTests extends AbstractSerializingDataFrameTestCase<PivotConfig> {

    public static PivotConfig randomPivotConfig() {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(),
            AggregationConfigTests.randomAggregationConfig(),
            randomBoolean() ? null : randomIntBetween(10, 10_000));
    }

    public static PivotConfig randomInvalidPivotConfig() {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(),
            AggregationConfigTests.randomInvalidAggregationConfig(),
            randomBoolean() ? null : randomIntBetween(10, 10_000));
    }

    @Override
    protected PivotConfig doParseInstance(XContentParser parser) throws IOException {
        return PivotConfig.fromXContent(parser, false);
    }

    @Override
    protected PivotConfig createTestInstance() {
        return randomPivotConfig();
    }

    @Override
    protected Reader<PivotConfig> instanceReader() {
        return PivotConfig::new;
    }

    public void testAggsAbbreviations() throws IOException {
        String pivotAggs = "{"
                    + " \"group_by\": {"
                    + "   \"id\": {"
                    + "     \"terms\": {"
                    + "       \"field\": \"id\""
                    + "} } },"
                    + " \"aggs\": {"
                    + "   \"avg\": {"
                    + "     \"avg\": {"
                    + "       \"field\": \"points\""
                    + "} } } }";

        PivotConfig p1 = createPivotConfigFromString(pivotAggs, false);
        String pivotAggregations = pivotAggs.replace("aggs", "aggregations");
        assertNotEquals(pivotAggs, pivotAggregations);
        PivotConfig p2 = createPivotConfigFromString(pivotAggregations, false);
        assertEquals(p1,p2);
    }

    public void testMissingAggs() throws IOException {
        String pivot = "{"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } } }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));
    }

    public void testEmptyAggs() throws IOException {
        String pivot = "{"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } },"
                + "\"aggs\": {}"
                + " }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));

        // lenient passes but reports invalid
        PivotConfig pivotConfig = createPivotConfigFromString(pivot, true);
        assertFalse(pivotConfig.isValid());
    }

    public void testEmptyGroupBy() throws IOException {
        String pivot = "{"
                + " \"group_by\": {},"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));

        // lenient passes but reports invalid
        PivotConfig pivotConfig = createPivotConfigFromString(pivot, true);
        assertFalse(pivotConfig.isValid());
    }

    public void testMissingGroupBy() {
        String pivot = "{"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));
    }

    public void testDoubleAggs() {
        String pivot = "{"
                + " \"group_by\": {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } },"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } },"
                + " \"aggregations\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } }"
                + "}";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));
    }

    public void testValidAggNames() throws IOException {
        String pivotAggs = "{"
            + " \"group_by\": {"
            + "   \"user.id.field\": {"
            + "     \"terms\": {"
            + "       \"field\": \"id\""
            + "} } },"
            + " \"aggs\": {"
            + "   \"avg.field.value\": {"
            + "     \"avg\": {"
            + "       \"field\": \"points\""
            + "} } } }";
        PivotConfig pivotConfig = createPivotConfigFromString(pivotAggs, true);
        assertTrue(pivotConfig.isValid());
        List<String> fieldValidation = pivotConfig.aggFieldValidation();
        assertTrue(fieldValidation.isEmpty());
    }

    public void testAggNameValidationsWithoutIssues() {
        String prefix = randomAlphaOfLength(10) + "1";
        String prefix2 = randomAlphaOfLength(10) + "2";
        String nestedField1 = randomAlphaOfLength(10) + "3";
        String nestedField2 = randomAlphaOfLength(10) + "4";

        assertThat(PivotConfig.aggFieldValidation(Arrays.asList(prefix + nestedField1 + nestedField2,
            prefix + nestedField1,
            prefix,
            prefix2)), is(empty()));

        assertThat(PivotConfig.aggFieldValidation(
            Arrays.asList(
                dotJoin(prefix, nestedField1, nestedField2),
                dotJoin(nestedField1, nestedField2),
                nestedField2,
                prefix2)), is(empty()));
    }

    public void testAggNameValidationsWithDuplicatesAndNestingIssues() {
        String prefix = randomAlphaOfLength(10) + "1";
        String prefix2 = randomAlphaOfLength(10) + "2";
        String nestedField1 = randomAlphaOfLength(10) + "3";
        String nestedField2 = randomAlphaOfLength(10) + "4";

        List<String> failures = PivotConfig.aggFieldValidation(
            Arrays.asList(
                dotJoin(prefix, nestedField1, nestedField2),
                dotJoin(prefix, nestedField2),
                dotJoin(prefix, nestedField1),
                dotJoin(prefix2, nestedField1),
                dotJoin(prefix2, nestedField1),
                prefix2));

        assertThat(failures,
            containsInAnyOrder("duplicate field [" + dotJoin(prefix2, nestedField1) + "] detected",
                "field [" + prefix2 + "] cannot be both an object and a field",
                "field [" + dotJoin(prefix, nestedField1) + "] cannot be both an object and a field"));
    }

    private static String dotJoin(String... fields) {
        return Strings.arrayToDelimitedString(fields, ".");
    }

    private PivotConfig createPivotConfigFromString(String json, boolean lenient) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return PivotConfig.fromXContent(parser, lenient);
    }
}
