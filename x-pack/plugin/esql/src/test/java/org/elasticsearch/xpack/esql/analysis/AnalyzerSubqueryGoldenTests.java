/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Golden (characterization) tests for the analyzed plans produced by subquery-in-{@code FROM} scenarios (the {@code UnionAll} /
 * {@code ViewUnionAll} planning). These were migrated from the per-node structural assertions in {@code AnalyzerSubqueryTests}.
 */
public class AnalyzerSubqueryGoldenTests extends GoldenTestCase {

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);

    private static final String SALARIES_INT_RESOURCE = "s3://bucket/salaries_int.parquet";
    private static final String SALARIES_LONG_RESOURCE = "s3://bucket/salaries_long.parquet";

    private static void requireExternalDatasetSupport() {
        assumeTrue("Requires external dataset in FROM command support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
    }

    private static void requireRowSubquerySupport() {
        assumeTrue("Requires subquery with row as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    private static void requireTsSubquerySupport() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
    }

    private static void requireTsMixedWithNonTsSourcesFix() {
        assumeTrue("Requires fix for mixing TS with non-TS sources", EsqlCapabilities.Cap.FIX_TS_MIXED_WITH_NON_TS_SOURCES.isEnabled());
    }

    private static void requireNullifySupport() {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
    }

    /**
     * A negotiated transport version that supports {@code dimension_values}. Time-series {@code rate(...) BY <dimension>} aggregations are
     * rewritten to either {@code DIMENSIONVALUES} or {@code VALUES} depending on the version, so pinning keeps the snapshot deterministic.
     */
    private static TransportVersion dimensionValuesVersion() {
        return TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION);
    }

    // -- basic subquery / view in FROM --

    public void testSubqueryInFrom() {
        runGoldenTest("""
            FROM employees, (FROM languages | WHERE language_code > 1)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """, STAGES);
    }

    public void testViewInFrom() {
        runGoldenTest("""
            FROM employees, view
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """, STAGES, Map.of("view", "FROM languages | WHERE language_code > 1"));
    }

    public void testSubqueryInFromWithoutMainIndexPattern() {
        runGoldenTest("""
            FROM (FROM languages | WHERE language_code > 1)
            | WHERE language_name is not null
            """, STAGES);
    }

    public void testViewInFromWithoutMainIndexPattern() {
        runGoldenTest("""
            FROM view
            | WHERE language_name is not null
            """, STAGES, Map.of("view", "FROM languages | WHERE language_code > 1"));
    }

    public void testMultipleSubqueriesInFrom() {
        runGoldenTest("""
            FROM employees
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            , (FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """, STAGES);
    }

    public void testMultipleViewsInFrom() {
        runGoldenTest(
            """
                FROM employees, view1, view2, view3
                | WHERE emp_no > 10000
                | STATS count(*) by emp_no, language_code
                | RENAME emp_no AS empNo, language_code AS languageCode
                | MV_EXPAND languageCode
                """,
            STAGES,
            Map.of(
                "view1",
                "FROM languages | WHERE language_code > 10 | RENAME language_name as languageName",
                "view2",
                "FROM sample_data | STATS max(@timestamp)",
                "view3",
                "FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code"
            )
        );
    }

    public void testMultipleSubqueryInFromWithoutMainIndexPattern() {
        runGoldenTest("""
            FROM (FROM employees | EVAL language_code = languages | LOOKUP JOIN languages_lookup ON language_code)
            , (FROM languages | WHERE language_code > 10 | RENAME language_name as languageName)
            , (FROM sample_data | STATS max(@timestamp))
            | WHERE emp_no > 10000
            | STATS count(*) by emp_no, language_code
            | RENAME emp_no AS empNo, language_code AS languageCode
            | MV_EXPAND languageCode
            """, STAGES);
    }

    public void testNestedSubqueryInFrom() {
        runGoldenTest("""
            FROM employees, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10)
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """, STAGES);
    }

    public void testNestedSubqueryInFromWithMetadata() {
        runGoldenTest("""
            FROM employees, (FROM languages, (FROM sample_data | STATS count(*)) | WHERE language_code > 10) metadata _index
            | WHERE emp_no > 10000
            | SORT emp_no, language_code
            """, STAGES);
    }

    public void testNestedSubqueriesInFromWithoutMainIndexPattern() {
        runGoldenTest("""
            FROM (FROM employees, (FROM sample_data | STATS count(*)) | WHERE emp_no > 10)
            | WHERE languages is not null
            | SORT emp_no, languages
            """, STAGES);
    }

    // -- mixed / conflicting data types across subquery branches --

    public void testMixedDataTypesInSubquery() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | SORT emp_no
            """, STAGES);
    }

    public void testMixedDataTypesWithExplicitCastingInSubquery() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible | WHERE languages > 0)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 10000
            | EVAL still_hired = still_hired::string, is_rehired = is_rehired::string
            | SORT still_hired, is_rehired
            """, STAGES);
    }

    public void testSubqueryWithUnionAllOutputOverwritten() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible | WHERE languages > 1)
            | EVAL emp_no = languages::long
            | WHERE emp_no > 1
            | SORT emp_no
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromSubqueries() {
        runGoldenTest("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1) | keep client_ip
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromSubqueriesWithoutUsageInMainQuery() {
        runGoldenTest("""
            FROM (FROM sample_data), (FROM sample_data | EVAL client_ip = 1)
            """, STAGES);
    }

    public void testUnionAllWithConflictingNumericTypesFromSubqueries() {
        runGoldenTest("""
            FROM employees, (FROM employees_incompatible) | keep emp_no
            """, STAGES);
    }

    // -- mixed data types across subquery branches sourced from external datasets --

    public void testUnionAllWithConflictingTypesFromExternalDatasetSubqueries() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | KEEP salary
            """);
    }

    public void testUnionAllWithConflictingTypesFromExternalDatasetSubqueriesWithoutUsage() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM (FROM salaries_int), (FROM salaries_long)
            """);
    }

    public void testExternalDatasetSubqueryConflictResolvedByCastInSubqueries() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM (FROM salaries_int | EVAL salary = salary::long), (FROM salaries_long)
            | KEEP salary
            """);
    }

    public void testExternalDatasetSubqueryConflictResolvedByCastInMainQuery() {
        requireExternalDatasetSupport();
        runExternalDatasetGoldenTest("""
            FROM (FROM salaries_int), (FROM salaries_long)
            | EVAL salary = salary::long
            | KEEP salary
            """);
    }

    // -- full text functions over a subquery in FROM --

    public void testSubqueryWithFullTextFunctionInMainQuery() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data | WHERE message:"error")
            | WHERE match(client_ip,"127.0.0.1")
            """, STAGES);
    }

    // -- RENAME / convert-function carry over through the UnionAll --

    public void testSubqueryWithRenameAndEvalWithConversionFunction() {
        runGoldenTest("""
            FROM employees, (FROM employees)
            | RENAME first_name AS fname
            | EVAL emp_no_str = to_string(emp_no)
            """, STAGES);
    }

    public void testSubqueryWithRenameOnSameFieldAsConvertFunction() {
        runGoldenTest("""
            FROM employees, (FROM employees)
            | RENAME emp_no AS id
            | EVAL id_str = to_string(id)
            """, STAGES);
    }

    public void testSubqueryWithConvertFunctionBeforeRenameOnSameField() {
        runGoldenTest("""
            FROM employees, (FROM employees)
            | EVAL emp_no_str = to_string(emp_no)
            | RENAME emp_no AS id
            """, STAGES);
    }

    public void testSubqueryWithRenameAndOtherProcessingCommandsWithConversionFunction() {
        runGoldenTest("""
            FROM multi_column_joinable, (FROM multi_column_joinable)
            | RENAME extra2 AS other2
            | LOOKUP JOIN multi_column_joinable_lookup ON id_int, other2
            | MV_EXPAND other2
            | STATS absent(to_string(id_int))
            """, STAGES);
    }

    // -- ROW source command inside a subquery in FROM --

    public void testRowSubqueryInFrom() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW x = 1)
            | WHERE emp_no > 10000
            """, STAGES);
    }

    public void testRowSubqueryInFromWithProcessingCommandsInSubquery() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW x = 1 | EVAL y = x + 1 | WHERE y > 0)
            """, STAGES);
    }

    public void testRowSubqueryInFromWithoutMainIndexPattern() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW x = 1 | EVAL y = x + 1)
            | WHERE y > 0
            """, STAGES);
    }

    public void testMixedRowAndFromSubqueriesInFrom() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees
            , (ROW x = 1)
            , (FROM languages | WHERE language_code > 1)
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromRowSubqueries() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW x = 1), (ROW x = "abc")
            | keep x
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromRowSubqueriesWithoutUsageInMainQuery() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW x = 1), (ROW x = "abc")
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromMixedRowAndFromSubqueries() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (FROM sample_data), (ROW client_ip = 1)
            | keep client_ip
            """, STAGES);
    }

    public void testUnionAllWithConflictingTypesFromRowSubqueryAndMainIndex() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW emp_no = "abc")
            | keep emp_no
            """, STAGES);
    }

    public void testMixedDataTypesInRowSubqueryWithExplicitCastingInside() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW emp_no = "1" | EVAL emp_no = emp_no::integer)
            | WHERE emp_no > 0
            """, STAGES);
    }

    public void testMixedDataTypesInRowSubqueryWithExplicitCastingOutside() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW emp_no = 1)
            | EVAL emp_no = emp_no::long
            | WHERE emp_no > 1000
            """, STAGES);
    }

    public void testMixedDataTypesInRowSubqueryWithExplicitCastingInsideAndOutside() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW emp_no = "1" | EVAL emp_no = emp_no::integer)
            | EVAL emp_no_long = emp_no::long
            | WHERE emp_no_long > 0
            """, STAGES);
    }

    public void testMultipleRowSubqueriesWithExplicitCastingInside() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW x = "1" | EVAL x = x::integer)
               , (ROW x = 1.5 | EVAL x = x::integer)
               , (ROW x = 3)
            """, STAGES);
    }

    public void testUnionAllWithMatchingTypesFromMultipleRowSubqueries() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW a = 1, b = "x"), (ROW a = 2, b = "y"), (ROW a = 3, b = "z")
            """, STAGES);
    }

    public void testTwoRowSubqueriesEachWithMixedScalarAndMultivalueFieldsMatchingTypes() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW a = 1, b = [10, 20]), (ROW a = [100, 200], b = 1)
            """, STAGES);
    }

    public void testTwoRowSubqueriesEachWithMixedScalarAndMultivalueFieldsConflictingTypes() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW a = 1, b = ["cat", "dog"]), (ROW a = [1.5, 2.5], b = true)
            | KEEP a, b
            """, STAGES);
    }

    public void testThreeRowSubqueriesWithDisjointFieldNamesMixedScalarAndMultivalue() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM (ROW a = 1, b = [10, 20, 30])
               , (ROW c = "hello", d = [true, false])
               , (ROW e = [1.5, -2.5], f = 100)
            """, STAGES);
    }

    public void testIndexPatternWithMixedRowSubqueriesAndConflictingTypes() {
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM employees, (ROW x = 1, y = ["cat", "dog"]), (ROW x = [1.5, -2.5], y = true)
            """, STAGES);
    }

    // TimeSeries indices referenced by from command inside subquery

    public void testSubqueryWithTimeSeriesIndexInMainQuery() {
        runGoldenTest("""
            FROM k8s-downsampled, (FROM sample_data), (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, STAGES);
    }

    public void testSubqueryWithTimeSeriesIndexInSubquery() {
        runGoldenTest("""
            FROM
                sample_data,
                (FROM k8s-downsampled | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, STAGES);
    }

    public void testSubqueryWithTimeSeriesIndexInMainQueryAndSubquery() {
        runGoldenTest("""
            FROM
                k8s-downsampled,
                (FROM k8s-downsampled | EVAL a = TO_AGGREGATE_METRIC_DOUBLE(1) | INLINE STATS tx_max = MAX(network.eth0.tx) BY pod),
                (FROM sample_data | WHERE client_ip == "127.0.0.1")
            | WHERE @timestamp > "2025-10-07"
            """, STAGES);
    }

    // -- TS source inside a FROM subquery

    public void testTSSubqueryInFrom() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM employees, (TS k8s-downsampled | WHERE @timestamp > "2025-10-07")
            """, STAGES);
    }

    public void testTSSubqueryInFromWithoutMainIndexPattern() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM (TS k8s-downsampled | WHERE @timestamp > "2025-10-07")
            """, STAGES);
    }

    public void testTSSubqueryWithTimeSeriesAggregate() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM employees, (TS k8s-downsampled | STATS m = max(rate(network.total_bytes_in)) BY cluster, pod)
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryInFromWithOuterTimeSeriesAggregate() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM (TS k8s-downsampled)
            | STATS x = last_over_time(event) BY time_bucket = bucket(@timestamp, 1 day)
            """, STAGES, dimensionValuesVersion());
    }

    public void testMultipleSubqueriesInFromWithTS() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                employees,
                (TS k8s-downsampled | STATS rate = max(rate(network.total_bytes_in)) BY cluster),
                (FROM sample_data | STATS cnt = count(*))
            """, STAGES, dimensionValuesVersion());
    }

    public void testMultipleSubqueriesInFromWithMixedTsRowAndFromSubqueries() {
        requireTsSubquerySupport();
        requireRowSubquerySupport();
        runGoldenTest("""
            FROM
                employees,
                (TS k8s-downsampled | STATS rate = max(rate(network.total_bytes_in)) BY cluster),
                (FROM sample_data | STATS cnt = count(*)),
                (ROW synthetic = 1)
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryWithProcessingCommands() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                employees,
                (TS k8s-downsampled
                 | WHERE @timestamp > "2025-10-07"
                 | EVAL doubled = network.cost * 2
                 | KEEP cluster, pod, doubled)
            """, STAGES);
    }

    public void testNestedTSSubqueryInFrom() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM employees, (FROM sample_data, (TS k8s-downsampled | WHERE @timestamp > "2025-10-07"))
            """, STAGES);
    }

    public void testTSAndFromSubqueriesInFromWithoutMainIndexPattern() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM (TS k8s-downsampled | WHERE @timestamp > "2025-10-07"), (FROM sample_data)
            """, STAGES);
    }

    public void testTSSubqueryWithByWithoutAndFromSubquery() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                (TS k8s-downsampled | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
                (FROM sample_data)
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryWithByWithoutInFromCommand() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                employees,
                (TS k8s-downsampled | STATS m = max(rate(network.total_bytes_in)) BY WITHOUT(pod)),
                (FROM sample_data)
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryWithConflictingTypesInUnionAll() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                (TS k8s-downsampled | STATS m = max(rate(network.total_bytes_in)) BY cluster),
                (FROM sample_data | EVAL m = "abc")
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryWithConflictingTypesAndExplicitCast() {
        requireTsSubquerySupport();
        runGoldenTest("""
            FROM
                (TS k8s-downsampled | STATS m = max(rate(network.total_bytes_in)) BY cluster),
                (FROM sample_data | EVAL m = "abc")
            | EVAL m = m::string
            | KEEP m
            """, STAGES, dimensionValuesVersion());
    }

    public void testTSSubqueryWithNumericConflict() {
        requireTsSubquerySupport();
        // Without an explicit cast: LONG vs DOUBLE -> UNSUPPORTED.
        runGoldenTest("""
            FROM (TS k8s-downsampled | STATS m = sum(last_over_time(network.bytes_in))),
              (FROM sample_data | EVAL m = 1.5)
            """, STAGES);
    }

    public void testTSSubqueryWithNumericConflictAndExplicitCast() {
        requireTsSubquerySupport();
        // With explicit cast m::double: TODOUBLE is pushed into each branch.
        runGoldenTest("""
            FROM
                (TS k8s-downsampled | STATS m = sum(last_over_time(network.bytes_in))),
                (FROM sample_data | EVAL m = 1.5)
            | EVAL m = m::double
            | KEEP m
            """, STAGES);
    }

    // -- TS subquery mixed with non-TS sources: outer STATS must produce plain Aggregate --

    public void testTsSubqueryMixedWithStandardSubquery() {
        requireTsMixedWithNonTsSourcesFix();
        runGoldenTest("""
            FROM (TS k8s-downsampled), (FROM sample_data) | STATS count(*)
            """, STAGES);
    }

    public void testTwoTsSubqueriesInFrom() {
        requireTsMixedWithNonTsSourcesFix();
        runGoldenTest("""
            FROM (TS k8s-downsampled), (TS k8s-downsampled) | STATS count(*)
            """, STAGES);
    }

    public void testTsSubqueryMixedWithDirectIndex() {
        requireTsMixedWithNonTsSourcesFix();
        runGoldenTest("""
            FROM (TS k8s-downsampled), sample_data | STATS count(*)
            """, STAGES);
    }

    public void testTsSubqueryMixedWithView() {
        requireTsMixedWithNonTsSourcesFix();
        runGoldenTest("""
            FROM (TS k8s-downsampled), my_view | STATS count(*)
            """, STAGES, Map.of("my_view", "FROM sample_data | STATS total = COUNT() BY message"));
    }

    // -- subquery union of a regular index, a time-series rate, and an external dataset --

    public void testSubqueryUnionOfIndexTimeSeriesRateAndExternalDataset() {
        requireTsSubquerySupport();
        requireExternalDatasetSupport();
        // `cluster` is a time-series dimension, so the rate aggregation is rewritten to DIMENSIONVALUES or VALUES depending on the
        // negotiated cluster version; pin a version supporting `dimension_values` so the snapshot stays deterministic.
        runExternalDatasetGoldenTest("""
            FROM (FROM sample_data | EVAL name = message | KEEP name),
                 (TS k8s | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster | EVAL name = cluster | KEEP name),
                 (FROM salaries_int | KEEP name)
            """, dimensionValuesVersion());
    }

    public void testSubqueryRenameKeepStarOnMissingColumnPreservesType() {
        runGoldenTest("""
            FROM employees, (FROM languages)
            | KEEP emp_no
            | RENAME emp_no AS x
            | KEEP *
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnMissingCounterFields() {
        runGoldenTest("""
            FROM k8s-downsampled, (FROM languages)
            | KEEP network.total_bytes_in, network.eth0.tx
            | RENAME network.total_bytes_in AS x, network.eth0.tx AS y
            | KEEP *
            | SORT x
            """, STAGES);
    }

    public void testSubqueryRenameChainKeepStarOnMissingCounterField() {
        runGoldenTest("""
            FROM k8s-downsampled, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x, x as y
            | KEEP y
            | SORT y
            """, STAGES);
    }

    public void testSubqueryDoubleRenameKeepStarOnMissingCounterField() {
        runGoldenTest("""
            FROM k8s-downsampled, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x
            | RENAME x as y
            | KEEP *
            | SORT y
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnMissingCounterFieldsWithNullifyAndSort() {
        requireNullifySupport();
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM k8s-downsampled, (FROM languages)
            | KEEP network.total_bytes_in, network.eth0.tx
            | RENAME network.total_bytes_in AS x, network.eth0.tx AS y
            | KEEP *
            | SORT x
            """, STAGES);
    }

    public void testSubqueryRenameChainKeepStarOnMissingCounterFieldWithNullifyAndSort() {
        requireNullifySupport();
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM k8s-downsampled, (FROM languages)
            | KEEP network.total_bytes_in
            | RENAME network.total_bytes_in AS x, x as y
            | KEEP y
            | SORT y
            """, STAGES);
    }

    public void testSubqueryDoubleRenameDropStarOnMissingCounterFieldWithNullifyAndSort() {
        requireNullifySupport();
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM k8s-downsampled, (FROM languages)
            | DROP @timestamp, language_*
            | RENAME network.total_bytes_in AS x
            | RENAME x as y
            | KEEP *
            | SORT y
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnDateAndDateNanosTimestamp() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """, STAGES);
    }

    public void testSubqueryRenameChainKeepOnDateAndDateNanosTimestamp() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x, x AS y
            | KEEP y
            | SORT y
            """, STAGES);
    }

    public void testSubqueryDoubleRenameKeepStarOnDateAndDateNanosTimestamp() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | RENAME x AS y
            | KEEP *
            | SORT y
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnDateAndDateNanosTimestampWithNullify() {
        requireNullifySupport();
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_nanos)
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnDateAndLongTimestampWithExplicitCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """, STAGES);
    }

    public void testSubqueryRenameChainKeepOnDateAndLongTimestampWithExplicitCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x, x AS y
            | KEEP y
            | SORT y
            """, STAGES);
    }

    public void testSubqueryDoubleRenameKeepStarOnDateAndLongTimestampWithExplicitCast() {
        runGoldenTest("""
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | RENAME x AS y
            | KEEP *
            | SORT y
            """, STAGES);
    }

    public void testSubqueryRenameKeepOnDateAndLongTimestampWithExplicitCastAndNullify() {
        requireNullifySupport();
        runGoldenTest("""
            SET unmapped_fields="nullify";
            FROM sample_data, (FROM sample_data_ts_long)
            | EVAL @timestamp = @timestamp::long
            | KEEP @timestamp
            | RENAME @timestamp AS x
            | KEEP *
            | SORT x
            """, STAGES);
    }

    // -- helpers --

    /**
     * Runs a golden test for a query that mixes subqueries with external datasets, registering the {@code salaries_int}/
     * {@code salaries_long} datasets and their resolved schemas. The golden framework replays the production pipeline order from
     * {@code EsqlSession} (rewrite FROM dataset targets into external relations via {@code DatasetRewriter}), so a dataset branch resolves
     * to an {@code ExternalRelation} exactly like a real dataset subquery.
     */
    private void runExternalDatasetGoldenTest(String query) {
        builder(query).stages(STAGES).datasetMetadata(datasetMetadata()).externalSourceResolution(externalSourceResolution()).run();
    }

    /** As {@link #runExternalDatasetGoldenTest(String)} but pins the negotiated transport version (e.g. for time-series sub-branches). */
    private void runExternalDatasetGoldenTest(String query, TransportVersion transportVersion) {
        builder(query).stages(STAGES)
            .datasetMetadata(datasetMetadata())
            .externalSourceResolution(externalSourceResolution())
            .transportVersion(transportVersion)
            .run();
    }

    private static ProjectMetadata datasetMetadata() {
        DataSource dataSource = new DataSource("external_ds", "test", null, Map.of());
        Dataset intDataset = new Dataset("salaries_int", new DataSourceReference("external_ds"), SALARIES_INT_RESOURCE, null, Map.of());
        Dataset longDataset = new Dataset("salaries_long", new DataSourceReference("external_ds"), SALARIES_LONG_RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("external_ds", dataSource)))
            .datasets(Map.of("salaries_int", intDataset, "salaries_long", longDataset))
            .build();
    }

    private static ExternalSourceResolution externalSourceResolution() {
        return new ExternalSourceResolution(
            Map.of(
                SALARIES_INT_RESOURCE,
                externalSource(SALARIES_INT_RESOURCE, DataType.INTEGER),
                SALARIES_LONG_RESOURCE,
                externalSource(SALARIES_LONG_RESOURCE, DataType.LONG)
            )
        );
    }

    /** A resolved external source named {@code emp_no}/{@code name}/{@code salary} with the given salary type. */
    private static ExternalSourceResolution.ResolvedSource externalSource(String path, DataType salaryType) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("name", DataType.KEYWORD),
            referenceAttribute("salary", salaryType)
        );
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return path;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
        return new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of());
    }
}
