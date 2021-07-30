/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;

public class PivotConfigTests extends AbstractSerializingTransformTestCase<PivotConfig> {

    public static PivotConfig randomPivotConfigWithDeprecatedFields() {
        return randomPivotConfigWithDeprecatedFields(Version.CURRENT);
    }

    public static PivotConfig randomPivotConfigWithDeprecatedFields(Version version) {
        return new PivotConfig(
            GroupConfigTests.randomGroupConfig(version),
            AggregationConfigTests.randomAggregationConfig(),
            randomIntBetween(10, 10_000) // deprecated
        );
    }

    public static PivotConfig randomPivotConfig() {
        return randomPivotConfig(Version.CURRENT);
    }

    public static PivotConfig randomPivotConfig(Version version) {
        return new PivotConfig(
            GroupConfigTests.randomGroupConfig(version),
            AggregationConfigTests.randomAggregationConfig(),
            null // deprecated
        );
    }

    public static PivotConfig randomInvalidPivotConfig() {
        return new PivotConfig(
            GroupConfigTests.randomGroupConfig(),
            AggregationConfigTests.randomInvalidAggregationConfig(),
            null // deprecated
        );
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
        assertEquals(p1, p2);
    }

    public void testMissingAggs() throws IOException {
        String pivot = "{" + " \"group_by\": {" + "   \"id\": {" + "     \"terms\": {" + "       \"field\": \"id\"" + "} } } }";

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
        ValidationException validationException = pivotConfig.validate(null);
        assertThat(validationException, is(notNullValue()));
        assertThat(validationException.getMessage(), containsString("pivot.aggregations must not be null"));
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
        ValidationException validationException = pivotConfig.validate(null);
        assertThat(validationException, is(notNullValue()));
        assertThat(validationException.getMessage(), containsString("pivot.groups must not be null"));
    }

    public void testMissingGroupBy() {
        String pivot = "{" + " \"aggs\": {" + "   \"avg\": {" + "     \"avg\": {" + "       \"field\": \"points\"" + "} } } }";

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

    public void testAggDuplicates() throws IOException {
        String pivot = "{"
            + " \"group_by\": {"
            + "   \"id\": {"
            + "     \"terms\": {"
            + "       \"field\": \"id\""
            + "} } },"
            + " \"aggs\": {"
            + "   \"points\": {"
            + "     \"max\": {"
            + "       \"field\": \"points\""
            + "} },"
            + "   \"points\": {"
            + "     \"min\": {"
            + "       \"field\": \"points\""
            + "} } }"
            + "}";

        // this throws early in the agg framework
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
        assertNull(pivotConfig.validate(null));
    }

    public void testValidAggNamesNested() throws IOException {
        String pivotAggs = "{"
            + "\"group_by\": {"
            + "  \"timestamp\": {"
            + "    \"date_histogram\": {"
            + "      \"field\": \"timestamp\","
            + "      \"fixed_interval\": \"1d\""
            + "    }"
            + "  }"
            + "},"
            + "\"aggregations\": {"
            + "  \"jp\": {"
            + "    \"filter\": {"
            + "      \"term\": {"
            + "        \"geo.src\": \"JP\""
            + "      }"
            + "    },"
            + "    \"aggs\": {"
            + "      \"os.dc\": {"
            + "        \"cardinality\": {"
            + "          \"field\": \"machine.os.keyword\""
            + "        }"
            + "      }"
            + "    }"
            + "  },"
            + "  \"us\": {"
            + "    \"filter\": {"
            + "      \"term\": {"
            + "        \"geo.src\": \"US\""
            + "      }"
            + "    },"
            + "    \"aggs\": {"
            + "      \"os.dc\": {"
            + "        \"cardinality\": {"
            + "          \"field\": \"machine.os.keyword\""
            + "} } } } } }";

        PivotConfig pivotConfig = createPivotConfigFromString(pivotAggs, true);
        assertNull(pivotConfig.validate(null));
    }

    public void testValidAggNamesNestedTwice() throws IOException {
        String pivotAggs = "{"
            + "    \"group_by\": {"
            + "      \"timestamp\": {"
            + "        \"date_histogram\": {"
            + "          \"field\": \"timestamp\","
            + "          \"fixed_interval\": \"1d\""
            + "        }"
            + "      }"
            + "    },"
            + "    \"aggregations\": {"
            + "      \"jp\": {"
            + "        \"filter\": {"
            + "          \"term\": {"
            + "            \"geo.src\": \"JP\""
            + "          }"
            + "        },"
            + "        \"aggs\": {"
            + "          \"us\": {"
            + "            \"filter\": {"
            + "              \"term\": {"
            + "                \"geo.dest\": \"US\""
            + "              }"
            + "            },"
            + "            \"aggs\": {"
            + "              \"os.dc\": {"
            + "                \"cardinality\": {"
            + "                  \"field\": \"machine.os.keyword\""
            + "                }"
            + "              }"
            + "            }"
            + "          }"
            + "        }"
            + "      },"
            + "      \"us\": {"
            + "        \"filter\": {"
            + "          \"term\": {"
            + "            \"geo.src\": \"US\""
            + "          }"
            + "        },"
            + "        \"aggs\": {"
            + "          \"jp\": {"
            + "            \"filter\": {"
            + "              \"term\": {"
            + "                \"geo.dest\": \"JP\""
            + "              }"
            + "            },"
            + "            \"aggs\": {"
            + "              \"os.dc\": {"
            + "                \"cardinality\": {"
            + "                  \"field\": \"machine.os.keyword\""
            + "                }"
            + "              }"
            + "            }"
            + "          }"
            + "        }"
            + "      }"
            + "    }"
            + "  }";

        PivotConfig pivotConfig = createPivotConfigFromString(pivotAggs, true);
        assertNull(pivotConfig.validate(null));
    }

    public void testInValidAggNamesNestedTwice() throws IOException {
        String pivotAggs = "{"
            + "    \"group_by\": {"
            + "      \"jp.us.os.dc\": {"
            + "        \"date_histogram\": {"
            + "          \"field\": \"timestamp\","
            + "          \"fixed_interval\": \"1d\""
            + "        }"
            + "      }"
            + "    },"
            + "    \"aggregations\": {"
            + "      \"jp\": {"
            + "        \"filter\": {"
            + "          \"term\": {"
            + "            \"geo.src\": \"JP\""
            + "          }"
            + "        },"
            + "        \"aggs\": {"
            + "          \"us\": {"
            + "            \"filter\": {"
            + "              \"term\": {"
            + "                \"geo.dest\": \"US\""
            + "              }"
            + "            },"
            + "            \"aggs\": {"
            + "              \"os.dc\": {"
            + "                \"cardinality\": {"
            + "                  \"field\": \"machine.os.keyword\""
            + "                }"
            + "              }"
            + "            }"
            + "          }"
            + "        }"
            + "      },"
            + "      \"us\": {"
            + "        \"filter\": {"
            + "          \"term\": {"
            + "            \"geo.src\": \"US\""
            + "          }"
            + "        },"
            + "        \"aggs\": {"
            + "          \"jp\": {"
            + "            \"filter\": {"
            + "              \"term\": {"
            + "                \"geo.dest\": \"JP\""
            + "              }"
            + "            },"
            + "            \"aggs\": {"
            + "              \"os.dc\": {"
            + "                \"cardinality\": {"
            + "                  \"field\": \"machine.os.keyword\""
            + "                }"
            + "              }"
            + "            }"
            + "          }"
            + "        }"
            + "      }"
            + "    }"
            + "  }";

        PivotConfig pivotConfig = createPivotConfigFromString(pivotAggs, true);
        ValidationException validationException = pivotConfig.validate(null);
        assertThat(validationException, is(notNullValue()));
        assertThat(validationException.getMessage(), containsString("duplicate field [jp.us.os.dc] detected"));
    }

    public void testAggNameValidationsWithoutIssues() {
        String prefix = randomAlphaOfLength(10) + "1";
        String prefix2 = randomAlphaOfLength(10) + "2";
        String nestedField1 = randomAlphaOfLength(10) + "3";
        String nestedField2 = randomAlphaOfLength(10) + "4";

        assertThat(
            PivotConfig.aggFieldValidation(Arrays.asList(prefix + nestedField1 + nestedField2, prefix + nestedField1, prefix, prefix2)),
            is(empty())
        );

        assertThat(
            PivotConfig.aggFieldValidation(
                Arrays.asList(dotJoin(prefix, nestedField1, nestedField2), dotJoin(nestedField1, nestedField2), nestedField2, prefix2)
            ),
            is(empty())
        );
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
                prefix2
            )
        );

        assertThat(
            failures,
            containsInAnyOrder(
                "duplicate field [" + dotJoin(prefix2, nestedField1) + "] detected",
                "field [" + prefix2 + "] cannot be both an object and a field",
                "field [" + dotJoin(prefix, nestedField1) + "] cannot be both an object and a field"
            )
        );
    }

    public void testAggNameValidationsWithInvalidFieldnames() {
        List<String> failures = PivotConfig.aggFieldValidation(Arrays.asList(".at_start", "at_end.", ".start_and_end."));

        assertThat(
            failures,
            containsInAnyOrder(
                "field [.at_start] must not start with '.'",
                "field [at_end.] must not end with '.'",
                "field [.start_and_end.] must not start with '.'",
                "field [.start_and_end.] must not end with '.'"
            )
        );
    }

    public void testDeprecation() {
        PivotConfig pivotConfig = randomPivotConfigWithDeprecatedFields();
        assertWarnings("[max_page_search_size] is deprecated inside pivot please use settings instead");
    }

    private static String dotJoin(String... fields) {
        return Strings.arrayToDelimitedString(fields, ".");
    }

    private PivotConfig createPivotConfigFromString(String json, boolean lenient) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return PivotConfig.fromXContent(parser, lenient);
    }
}
