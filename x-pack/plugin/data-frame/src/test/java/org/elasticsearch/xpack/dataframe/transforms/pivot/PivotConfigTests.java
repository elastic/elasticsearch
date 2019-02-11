/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PivotConfigTests extends AbstractSerializingDataFrameTestCase<PivotConfig> {

    public static PivotConfig randomPivotConfig() {
        List<GroupConfig> groups = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            groups.add(GroupConfigTests.randomGroupConfig());
        }

        return new PivotConfig(groups, AggregationConfigTests.randomAggregationConfig());
    }

    public static PivotConfig randomInvalidPivotConfig() {
        List<GroupConfig> groups = new ArrayList<>();

        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            groups.add(GroupConfigTests.randomGroupConfig());
        }

        return new PivotConfig(groups, AggregationConfigTests.randomInvalidAggregationConfig());
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
                    + " \"group_by\": [ {"
                    + "   \"id\": {"
                    + "     \"terms\": {"
                    + "       \"field\": \"id\""
                    + "} } } ],"
                    + " \"aggs\": {"
                    + "   \"avg\": {"
                    + "     \"avg\": {"
                    + "       \"field\": \"points\""
                    + "} } } }";

        PivotConfig p1 = createPivotConfigFromString(pivotAggs);
        String pivotAggregations = pivotAggs.replace("aggs", "aggregations");
        assertNotEquals(pivotAggs, pivotAggregations);
        PivotConfig p2 = createPivotConfigFromString(pivotAggregations);
        assertEquals(p1,p2);
    }

    public void testMissingAggs() throws IOException {
        String pivot = "{"
                + " \"group_by\": [ {"
                + "   \"id\": {"
                + "     \"terms\": {"
                + "       \"field\": \"id\""
                + "} } } ] }";

        expectThrows(IllegalArgumentException.class, () -> createPivotConfigFromString(pivot));
    }

    private PivotConfig createPivotConfigFromString(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        return PivotConfig.fromXContent(parser, false);
    }
}
