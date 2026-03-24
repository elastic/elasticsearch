/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.APPROXIMATION_V5;

/**
 * Tests for query approximation generated from existing CSV tests.
 * <p>
 * To be eligible for this test, a CSV test must contain STATS (since query
 * approximation is only supported for queries containing STATS).
 */
public abstract class GenerativeApproximationRestTest extends EsqlSpecTestCase {
    public GenerativeApproximationRestTest(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected void doTest() throws Throwable {
        // The query from the test may not be supported for query approximation.
        // Therefore, there may be an "approximation is not supported" warning.
        // For simplicity, we just allow all warnings here.
        testCase.allowAllWarnings();

        doTest("SET approximation=true; " + testCase.query);
    }

    @Override
    protected void assertResults(
        CsvTestUtils.ExpectedResults expected,
        List<Map<String, String>> actualColumns,
        List<List<Object>> actualValues,
        Logger logger
    ) {
        // Query approximation may add additional columns starting with "_approximation_".
        // Since these columns are not present in the original CSV test, they are removed.
        for (int col = 0; col < actualColumns.size(); col++) {
            if (actualColumns.get(col).get("name").startsWith("_approximation_")) {
                actualColumns.remove(col);
                final int colFinal = col;
                actualValues.forEach(row -> row.remove(colFinal));
                col--;
            }
        }

        super.assertResults(expected, actualColumns, actualValues, logger);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);

        assumeFalse(
            "Approximation tests must not be approximated",
            testCase.requiredCapabilities.contains(APPROXIMATION_V5.capabilityName())
        );
        assumeTrue("Test must contain STATS to be included in approximation tests", testCase.query.toLowerCase().contains("stats"));

        // stats
        assumeFalse("...", "countNull".equals(testName));  // ClassCastException
        assumeFalse("...", "count_mv".equals(testName));  // ClassCastException
        assumeFalse("...", "count_where".equals(testName));  // ClassCastException
        assumeFalse("...", "docsCountWithExpression".equals(testName));  // ClassCastException
        assumeFalse("...", "isNullWithStatsCount_On_TextField".equals(testName));  // ClassCastException
        assumeFalse("...", "countStarGroupedTrunc".equals(testName));  // ClassCastException
        assumeFalse("...", "countStarGroupedTruncWithFilterOutsideStats".equals(testName));  // ClassCastException

        assumeFalse("...", "sumOfConst".equals(testName));  //  optimized incorrectly

        assumeFalse("...", "countWithConditions".equals(testName));  // can't read released page
        assumeFalse("...", "groupByNull".equals(testName));  // can't read released page
        assumeFalse("...", "groupByNullAndString".equals(testName));  // can't read released page
        assumeFalse("...", "avgWithConditions".equals(testName));  // can't read released page
        assumeFalse("...", "medianWithConditions".equals(testName));  // can't read released page
        assumeFalse("...", "percentileWithConditions".equals(testName));  // can't read released page
        assumeFalse("...", "weightedAvgWithConditions".equals(testName));  // can't read released page
        assumeFalse("...", "countMultiValuesRow".equals(testName));  // can't read released page
        assumeFalse("...", "groupByStringAndNull".equals(testName));  // can't read released page
        assumeFalse("...", "stdDevRow".equals(testName));  // can't read released page

        assumeFalse("...", "count_or_null".equals(testName));  // can't release already released object
        assumeFalse("...", "statsMvConstantGroupByAggExpr".equals(testName));  // can't release already released object
        assumeFalse("...", "weightedAvgConstant".equals(testName));  // can't release already released object
        assumeFalse("...", "weightedAvgBothConstantsMvWarning".equals(testName));  // can't release already released object
        assumeFalse("...", "sumRowMany".equals(testName));  // can't release already released object
        assumeFalse("...", "sumRowManyTwo".equals(testName));  // can't release already released object
        assumeFalse("...", "statsMvConstantGroupByWhere".equals(testName));  // can't release already released object
        assumeFalse("...", "statsMvConstantGroupByEval".equals(testName));  // can't release already released object

        // stats_percentile
        assumeFalse("...", "constantsRow".equals(testName));  // can't read released page
        assumeFalse("...", "percentile precision tests with row".equals(testName));  // can't read released page

        // unmapped-nullify
        assumeFalse("...", "statsFilteredAggs".equals(testName));  // ClassCastException
        assumeFalse("...", "statsFilteredAggsAndGroups".equals(testName));  // ClassCastException
        assumeFalse("...", "statsGroups".equals(testName));  // can't release already released object
        assumeFalse("...", "statsExpressions".equals(testName));  // can't release already released object
        assumeFalse("...", "statsExpressionsWithAliases".equals(testName));  // can't release already released object
        assumeFalse("...", "statsAggsGrouped".equals(testName));  // can't release already released object

        // string
        assumeFalse("...", "negatedNotEqualsToUpperFolded".equals(testName));  // ClassCastException

        // bucket
        assumeFalse("...", "bucketByYearLowBucketCount".equals(testName));  // ClassCastException
        assumeFalse("...", "bucketByWeekInString".equals(testName));  // ClassCastException
        assumeFalse("...", "docsBucketWeeklyHistogram".equals(testName));  // ClassCastException
        assumeFalse("...", "docsBucketWeeklyHistogramWithSpan".equals(testName));  // ClassCastException
        assumeFalse("...", "foldableBuckets".equals(testName));  // ClassCastException
        assumeFalse("...", "foldableBucketsInline".equals(testName));  // ClassCastException
        assumeFalse("...", "docsGettingStartedBucketStatsBy".equals(testName));  // ClassCastException
        assumeFalse("...", "docsBucketMonthlyHistogram".equals(testName));  // ClassCastException
        assumeFalse("...", "bucketNumericMixedTypes".equals(testName));  // can't release already released object
        assumeFalse("...", "binNumericMixedTypes".equals(testName));  // can't release already released object
        assumeFalse("...", "bucketNumericMixedTypesIntegerSpans".equals(testName));  // can't release already released object

        // tbucket
        assumeFalse("...", "tbucketWithNumericBucketsAndExplicitFromToCount".equals(testName));  // ClassCastException
        assumeFalse("...", "tbucketWithNumericBucketsAndExplicitFromToCountFinerGranularity".equals(testName));  // ClassCastException

        // drop
        assumeFalse("...", "dropGrouping".equals(testName));  // can't read released page
        assumeFalse("...", "dropGroupingMulti".equals(testName));  // can't read released page
        assumeFalse("...", "dropGroupingMulti2".equals(testName));  // can't read released page

        // categorize
        assumeFalse("...", "row rename".equals(testName));  // can't read released page
        assumeFalse("...", "row drop".equals(testName));  // can't read released page
        assumeFalse("...", "category value processing".equals(testName));  // can't read released page
        assumeFalse("...", "row aliases".equals(testName));  // can't read released page
        assumeFalse("...", "row aliases with keep".equals(testName));  // can't read released page
        assumeFalse("...", "skips stopwords".equals(testName));  // can't read released page

        // row
        assumeFalse("...", "rowStatsProjectGroupByLong".equals(testName));  // can't read released page
        assumeFalse("...", "rowStatsProjectGroupByDouble".equals(testName));  // can't read released page
        assumeFalse("...", "rowWithNullsInCount".equals(testName));  // can't read released page
        assumeFalse("...", "rowStatsProjectGroupByInt".equals(testName));  // can't read released page
        assumeFalse("...", "rowWithNullsInAvg".equals(testName));  // can't read released page
        assumeFalse("...", "rowStatsProjectGroupByKeyword".equals(testName));  // can't read released page
        assumeFalse("...", "rowWithNullsInAvg2".equals(testName));  // can't release already released object
        assumeFalse("...", "sum".equals(testName));  // can't release already released object

        // dense_vector
        assumeFalse("...", "denseVectorLiteralCount".equals(testName));  // can't read released page

        // dense_vector-bit
        assumeFalse("...", "denseVectorCount".equals(testName));  // can't read released page

        // ip
        assumeFalse("...", "toIpInAgg".equals(testName));  // can't release already released object
        assumeFalse("...", "toIpInAggOctal".equals(testName));  // can't release already released object

        // math
        assumeFalse("...", "roundToSalaryWindow".equals(testName));  // ClassCastException
        assumeFalse("...", "roundToNanos".equals(testName));  // ClassCastException
        assumeFalse("...", "roundToBirthWindow".equals(testName));  // ClassCastException
        assumeFalse("...", "functionUnderArithmeticOperationAggString".equals(testName));  // can't release already released object

        // spatial_shapes
        assumeFalse("...", "cartesianPolygonDisjointEmptyGeometry".equals(testName));  // ClassCastException

        // mv_expand
        assumeFalse("...", "expandAfterDuplicateAggs".equals(testName));  // optimized incorrectly
        assumeFalse("...", "expandAfterDuplicateAggs2".equals(testName));  // optimized incorrectly
        assumeFalse("...", "expandAfterDuplicateAggsMultirow".equals(testName));  // optimized incorrectly
        assumeFalse("...", "expandAfterDuplicateAggsAndEval".equals(testName));  // optimized incorrectly
        assumeFalse("...", "expandAfterDuplicateAggsComplex".equals(testName));  // optimized incorrectly
        assumeFalse("...", "explosionStats".equals(testName));  // can't read released page

        // median_absolute_deviation
        assumeFalse("...", "medianAbsoluteDeviationFold".equals(testName));  // can't release already released object
        assumeFalse("...", "medianAbsoluteDeviationWithConditions".equals(testName));  // can't read released page
    }
}
