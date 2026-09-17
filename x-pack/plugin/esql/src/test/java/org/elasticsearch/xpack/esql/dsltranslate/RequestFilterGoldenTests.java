/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

/**
 * Pins what every request-filter shape translates to, for every field type, as the analyzed plan with the translated
 * filter installed above the dataset. Each test puts one shape on every column as {@code must} arms, so a single
 * expected file shows the whole type axis: an arm the translator cannot express is dropped from the filter and is
 * simply absent from the expected plan.
 *
 * <p>This records the translation; it does not show the translation is right. Agreement with the index is the
 * business of {@code ExternalDatasetRequestFilterConformanceIT}, which runs each filter against both.
 */
public class RequestFilterGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public RequestFilterGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS);
    private static final String RESOURCE = "s3://bucket/every_type.parquet";

    /** A column, the value a shape compares it to, and a lower and upper bound around that value. */
    private record Column(String name, DataType type, Object value, Object lower, Object upper) {}

    // The dataset schema is declared here rather than through a registered mapping, so it can carry types a strict
    // declared-schema dataset cannot (text, version).
    private static final List<Column> COLUMNS = List.of(
        new Column("kw", DataType.KEYWORD, "b", "a", "c"),
        new Column("txt", DataType.TEXT, "b", "a", "c"),
        new Column("i", DataType.INTEGER, 5, 1, 9),
        new Column("l", DataType.LONG, 5L, 1L, 9L),
        new Column("d", DataType.DOUBLE, 1.5, 0.5, 2.5),
        new Column("b", DataType.BOOLEAN, true, false, true),
        new Column("dt", DataType.DATETIME, "2020-01-02", "2020-01-01", "2020-01-03"),
        new Column("dn", DataType.DATE_NANOS, "2020-01-02", "2020-01-01", "2020-01-03"),
        new Column("ul", DataType.UNSIGNED_LONG, 5L, 1L, 9L),
        new Column("ip", DataType.IP, "10.0.0.5", "10.0.0.1", "10.0.0.9"),
        new Column("v", DataType.VERSION, "1.2.3", "1.0.0", "2.0.0")
    );

    /**
     * An arm the translator cannot express is dropped with a warning. Which arms those are is exactly what the expected
     * plans record, so the warnings are not asserted here; the rewriter's own tests pin the warning text.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    // ---- every leaf construct, on every type ----

    public void testTermOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.termQuery(c.name(), c.value()));
    }

    public void testCaseInsensitiveTermOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.termQuery(c.name(), c.value()).caseInsensitive(true));
    }

    public void testTermsOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.termsQuery(c.name(), c.value(), c.lower()));
    }

    public void testMatchOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.matchQuery(c.name(), c.value()));
    }

    /** A value of the wrong type under a lenient match folds to no match instead of failing. */
    public void testLenientMatchWithMismatchedValueOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.matchQuery(c.name(), "not-a-value").lenient(true));
    }

    public void testMatchPhraseOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.matchPhraseQuery(c.name(), c.value()));
    }

    public void testExistsOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.existsQuery(c.name()));
    }

    public void testRangeWithLowerBoundOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.rangeQuery(c.name()).gte(c.lower()));
    }

    public void testRangeWithUpperBoundOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.rangeQuery(c.name()).lte(c.upper()));
    }

    public void testRangeWithBothBoundsOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.rangeQuery(c.name()).gte(c.lower()).lte(c.upper()));
    }

    public void testExclusiveRangeWithBothBoundsOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.rangeQuery(c.name()).gt(c.lower()).lt(c.upper()));
    }

    public void testRangeWithNoBoundsOnEveryType() {
        runOnEveryColumn(c -> QueryBuilders.rangeQuery(c.name()));
    }

    // ---- constructs that are not per-column ----

    public void testMatchAll() {
        runGoldenFilter(QueryBuilders.matchAllQuery());
    }

    public void testMatchNone() {
        runGoldenFilter(new MatchNoneQueryBuilder());
    }

    public void testMultiMatchShapes() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.multiMatchQuery("b", "kw", "txt"))
                .must(QueryBuilders.multiMatchQuery("b", "k*"))
                .must(QueryBuilders.multiMatchQuery("b"))
                .must(QueryBuilders.multiMatchQuery("b", "kw").type(MultiMatchQueryBuilder.Type.PHRASE))
                .must(QueryBuilders.multiMatchQuery("b", "kw").type(MultiMatchQueryBuilder.Type.CROSS_FIELDS))
        );
    }

    /** Range options: a time zone with a bound degrades, a format and inclusivity flags translate. */
    public void testRangeOptions() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("dt").gte("2020-01-01").timeZone("+01:00"))
                .must(QueryBuilders.rangeQuery("dt").gte("2020/01/01").format("yyyy/MM/dd"))
                .must(QueryBuilders.rangeQuery("i").gte(1).lte(9).includeLower(false).includeUpper(false))
                .must(QueryBuilders.rangeQuery("dt").timeZone("+01:00"))
        );
    }

    /** A field the dataset does not have: every construct binds it to null and folds to no match. */
    public void testEveryConstructOnAMissingField() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("nope", "b"))
                .must(QueryBuilders.termsQuery("nope", "a", "b"))
                .must(QueryBuilders.matchQuery("nope", "b"))
                .must(QueryBuilders.matchPhraseQuery("nope", "b"))
                .must(QueryBuilders.existsQuery("nope"))
                .must(QueryBuilders.rangeQuery("nope").gte(1).lte(9))
                .must(QueryBuilders.rangeQuery("nope").gte(true))
                .must(QueryBuilders.rangeQuery("nope"))
        );
    }

    /** Constructs outside the supported subset beside one that translates: only the term survives. */
    public void testUnsupportedConstructsAreDropped() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("kw", "b"))
                .must(QueryBuilders.wildcardQuery("kw", "b*"))
                .must(QueryBuilders.prefixQuery("kw", "b"))
                .must(QueryBuilders.regexpQuery("kw", "b.*"))
                .must(QueryBuilders.idsQuery().addIds("1"))
                .must(QueryBuilders.queryStringQuery("b"))
                .must(QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("kw", "b")))
                .must(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("kw", "b")))
                .must(QueryBuilders.boostingQuery(QueryBuilders.termQuery("kw", "b"), QueryBuilders.termQuery("kw", "a")))
        );
    }

    // ---- bool contexts ----

    public void testFilterContext() {
        runGoldenFilter(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("kw", "b")).filter(QueryBuilders.rangeQuery("i").gte(1)));
    }

    /** Each must_not clause is its own arm: the unsupported one is dropped, the supported one is still negated. */
    public void testMustNotContext() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("kw", "b"))
                .mustNot(QueryBuilders.wildcardQuery("kw", "b*"))
                .mustNot(QueryBuilders.rangeQuery("txt"))
        );
    }

    /** A must_not arm that is itself compound is dropped whole when anything inside it fails. */
    public void testCompoundMustNotArm() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("i", 5))
                .mustNot(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("kw", "b")).must(QueryBuilders.wildcardQuery("kw", "b*")))
        );
    }

    /** With no must or filter, the should group is required, and one unsupported arm drops the whole group. */
    public void testRequiredShouldGroup() {
        runGoldenFilter(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("kw", "b")).should(QueryBuilders.termQuery("i", 5)));
    }

    public void testRequiredShouldGroupWithAnUnsupportedArm() {
        runGoldenFilter(
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("kw", "b")).should(QueryBuilders.wildcardQuery("kw", "b*"))
        );
    }

    /** Beside a must, should arms gate scoring only, so they are not translated. */
    public void testNonRequiredShould() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("kw", "b"))
                .should(QueryBuilders.termQuery("i", 5))
                .should(QueryBuilders.wildcardQuery("kw", "b*"))
        );
    }

    public void testMinimumShouldMatch() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("kw", "b"))
                .should(QueryBuilders.termQuery("i", 5))
                .should(QueryBuilders.termQuery("l", 5L))
                .minimumShouldMatch(1)
        );
    }

    public void testUnsupportedMinimumShouldMatch() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("kw", "b"))
                .should(QueryBuilders.termQuery("i", 5))
                .should(QueryBuilders.termQuery("l", 5L))
                .minimumShouldMatch(2)
        );
    }

    public void testPureNegativeWithAdjustPureNegativeOff() {
        runGoldenFilter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("kw", "b")).adjustPureNegative(false));
    }

    public void testNestedBools() {
        runGoldenFilter(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery("kw", "b"))
                        .should(QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("i").gte(5)))
                )
                .mustNot(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("d")).filter(QueryBuilders.rangeQuery("dt")))
        );
    }

    // ---- helpers ----

    private void runOnEveryColumn(Function<Column, QueryBuilder> shape) {
        BoolQueryBuilder filter = QueryBuilders.boolQuery();
        for (Column column : COLUMNS) {
            filter.must(shape.apply(column));
        }
        runGoldenFilter(filter);
    }

    private void runGoldenFilter(QueryBuilder filter) {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        builder("FROM every_type").stages(STAGES)
            // Pinned: below the rewriter's version gate the filter is skipped, and the gate has its own tests.
            .transportVersion(TransportVersion.current())
            .datasetMetadata(datasetMetadata())
            .externalSourceResolution(externalSourceResolution())
            .requestFilter(filter)
            .run();
    }

    private static ProjectMetadata datasetMetadata() {
        DataSource dataSource = new DataSource("every_type_ds", "test", null, Map.of());
        Dataset dataset = new Dataset("every_type", new DataSourceReference("every_type_ds"), RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("every_type_ds", dataSource)))
            .datasets(Map.of("every_type", dataset))
            .build();
    }

    private static ExternalSourceResolution externalSourceResolution() {
        List<Attribute> schema = COLUMNS.stream().<Attribute>map(c -> referenceAttribute(c.name(), c.type())).toList();
        ExternalSourceMetadata metadata = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return RESOURCE;
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
        return new ExternalSourceResolution(
            Map.of(RESOURCE, new ExternalSourceResolution.ResolvedSource(metadata, FileList.UNRESOLVED, Map.of()))
        );
    }
}
