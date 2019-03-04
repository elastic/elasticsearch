/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;

public class PivotConfigTests extends AbstractSerializingDataFrameTestCase<PivotConfig> {

    public static PivotConfig randomPivotConfig() {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(), AggregationConfigTests.randomAggregationConfig());
    }

    public static PivotConfig randomInvalidPivotConfig() {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(), AggregationConfigTests.randomInvalidAggregationConfig());
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

    public void testMissingGroupBy() throws IOException {
        String pivot = "{"
                + " \"aggs\": {"
                + "   \"avg\": {"
                + "     \"avg\": {"
                + "       \"field\": \"points\""
                + "} } } }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot, false));
    }

    public void testDoubleAggs() throws IOException {
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

    private PivotConfig createPivotConfigFromString(String json, boolean lenient) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return PivotConfig.fromXContent(parser, lenient);
    }
}
