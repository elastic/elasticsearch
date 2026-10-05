/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromOrdsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.IntsBlockLoader;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.SearchExecutionContextHelper;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class EsPhysicalOperationProvidersTests extends MapperServiceTestCase {

    public void testNullsFilteredFieldInfos() {
        record TestCase(QueryBuilder query, List<String> nullsFilteredFields) {

        }
        List<TestCase> testCases = List.of(
            new TestCase(new MatchAllQueryBuilder(), List.of()),
            new TestCase(null, List.of()),
            new TestCase(new ExistsQueryBuilder("f1"), List.of("f1")),
            new TestCase(new ExistsQueryBuilder("f2"), List.of("f2")),
            new TestCase(
                new BoolQueryBuilder().should(new ExistsQueryBuilder("f1")).should(new ExistsQueryBuilder("f2")).minimumShouldMatch(1),
                List.of()
            ),
            new TestCase(
                new BoolQueryBuilder().should(new ExistsQueryBuilder("f1")).should(new ExistsQueryBuilder("f2")).minimumShouldMatch(2),
                List.of()
            ),
            new TestCase(new BoolQueryBuilder().filter(new ExistsQueryBuilder("f1")), List.of("f1")),
            new TestCase(new BoolQueryBuilder().filter(new ExistsQueryBuilder("f1")), List.of("f1")),
            new TestCase(new BoolQueryBuilder().filter(new ExistsQueryBuilder("f1")).should(new RangeQueryBuilder("f2")), List.of("f1")),
            new TestCase(new BoolQueryBuilder().filter(new ExistsQueryBuilder("f2")).mustNot(new RangeQueryBuilder("f1")), List.of("f2")),
            new TestCase(new TermQueryBuilder("f3", "v3"), List.of("f3")),
            new TestCase(new BoolQueryBuilder().filter(new ExistsQueryBuilder("f1")).must(new TermQueryBuilder("f1", "v1")), List.of("f1"))
        );
        EsPhysicalOperationProviders provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromSingleton<>(
                new EsPhysicalOperationProviders.DefaultShardContext(0, () -> {}, createMockContext(), AliasFilter.EMPTY)
            ),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        for (TestCase testCase : testCases) {
            EsQueryExec queryExec = new EsQueryExec(
                Source.EMPTY,
                "test",
                IndexMode.STANDARD,
                List.of(),
                null,
                null,
                10,
                List.of(new EsQueryExec.QueryBuilderAndTags(testCase.query, List.of()))
            );
            FieldExtractExec fieldExtractExec = new FieldExtractExec(
                Source.EMPTY,
                queryExec,
                List.of(
                    new FieldAttribute(
                        Source.EMPTY,
                        "f1",
                        new EsField("f1", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    ),
                    new FieldAttribute(
                        Source.EMPTY,
                        "f2",
                        new EsField("f2", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    ),
                    new FieldAttribute(
                        Source.EMPTY,
                        "f3",
                        new EsField("f3", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    ),
                    new FieldAttribute(
                        Source.EMPTY,
                        "f4",
                        new EsField("f4", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                    )
                ),
                MappedFieldType.FieldExtractPreference.NONE
            );
            var fieldInfos = provider.extractFields(fieldExtractExec);
            for (var field : fieldInfos) {
                assertThat(
                    "query: " + testCase.query + ", field: " + field.name(),
                    field.nullsFiltered(),
                    equalTo(testCase.nullsFilteredFields.contains(field.name()))
                );
            }
        }
    }

    /**
     * When unmapped_fields="load" and the unmapped field is a keyed subfield of a flattened field,
     * the shard context should resolve it from the mapping and use the keyed flattened block loader
     * instead of falling back to source.
     */
    public void testUnmappedFlattenedSubfieldUsesKeyedBlockLoader() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("resource")
                        .startObject("properties")
                        .startObject("attributes")
                        .field("type", "flattened")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        var defaultCtx = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var unmappedCtx = EsPhysicalOperationProviders.wrapWithUnmappedFieldContext(defaultCtx, "resource.attributes.host.name");

        MappedFieldType fieldType = unmappedCtx.fieldType("resource.attributes.host.name");
        assertThat(
            "Unmapped flattened subfield should resolve to KeyedFlattenedFieldType from shard mapping",
            fieldType,
            instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class)
        );

        BlockLoader blockLoader = unmappedCtx.blockLoader(
            "resource.attributes.host.name",
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            null,
            ByteSizeValue.ofKb(100),
            ByteSizeValue.ofKb(300)
        );
        assertThat(
            "Block loader for unmapped flattened subfield should be KeyedFlattenedDocValuesBlockLoader",
            blockLoader,
            instanceOf(KeyedFlattenedDocValuesBlockLoader.class)
        );
    }

    /**
     * {@code IndexResolver} applies {@code -nested} on the field-caps request, so the
     * coordinator never plans nested subfields. The shard must return constant nulls rather
     * than the nested field's
     * real doc-values loader, or a cross-index type skew crashes
     * {@code ValuesSourceReaderOperator.sanityCheckBlock} (see #154011).
     * If ES|QL later supports nested fields, these expectations will need updating.
     */
    public void testNestedSubfieldBlockLoaderReturnsNull() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("item")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        BlockLoader blockLoader = blockLoader(searchExecutionContext, "item.value");
        assertThat(
            "Nested subfields must not be extracted; field caps hides them from the coordinator",
            blockLoader,
            equalTo(ConstantNull.INSTANCE)
        );
    }

    /**
     * {@code include_in_root} copies nested values onto the root Lucene document, but
     * {@code IndexResolver} still applies {@code -nested}. Extraction must stay null to match planning.
     */
    public void testNestedSubfieldWithIncludeInRootBlockLoaderReturnsNull() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("item")
                        .field("type", "nested")
                        .field("include_in_root", true)
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        assertThat(blockLoader(searchExecutionContext, "item.value"), equalTo(ConstantNull.INSTANCE));
    }

    /**
     * Intermediate object mappers under a nested parent must still be treated as nested
     * ({@code NestedLookup.getNestedParent} walks past them).
     */
    public void testDeepNestedSubfieldBlockLoaderReturnsNull() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("a")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("b")
                        .startObject("properties")
                        .startObject("c")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        assertThat(blockLoader(searchExecutionContext, "a.b.c"), equalTo(ConstantNull.INSTANCE));
    }

    public void testObjectSubfieldBlockLoaderIsNotNull() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("item")
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        BlockLoader blockLoader = blockLoader(searchExecutionContext, "item.value");
        assertThat(blockLoader, instanceOf(IntsBlockLoader.class));
    }

    /**
     * COUNT-only is rewritten to {@code EsStatsQueryExec} and uses {@code querySupplierForField}.
     * Nested subfields are mapped, so {@code isMappedField} alone would still run EXISTS; with
     * {@code include_in_root} that matches parent docs and inflates COUNT.
     */
    public void testQuerySupplierForFieldSkipsNestedSubfield() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("item")
                        .field("type", "nested")
                        .field("include_in_root", true)
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "long")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        var shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        assertTrue("nested subfield is mapped; isMappedField alone would not skip the shard", shardContext.isMappedField("item.value"));
        var provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromSingleton<>(shardContext),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        org.elasticsearch.compute.lucene.ShardContext luceneShard = Mockito.mock(org.elasticsearch.compute.lucene.ShardContext.class);
        Mockito.when(luceneShard.index()).thenReturn(0);
        assertThat(
            "COUNT pushdown must not run EXISTS on nested subfields",
            provider.querySupplierForField(new ExistsQueryBuilder("item.value"), "item.value").apply(luceneShard),
            equalTo(List.of())
        );
    }

    /** A mapped nested subfield stays null under {@code unmapped_fields=load}. */
    public void testMappedNestedSubfieldStaysNullUnderUnmappedFieldContext() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                mapping(
                    b -> b.startObject("item")
                        .field("type", "nested")
                        .startObject("properties")
                        .startObject("value")
                        .field("type", "integer")
                        .endObject()
                        .endObject()
                        .endObject()
                )
            ),
            null
        );
        var defaultCtx = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var unmappedCtx = EsPhysicalOperationProviders.wrapWithUnmappedFieldContext(defaultCtx, "item.value");
        BlockLoader blockLoader = unmappedCtx.blockLoader(
            "item.value",
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            null,
            ByteSizeValue.ofKb(100),
            ByteSizeValue.ofKb(300)
        );
        assertThat(blockLoader, equalTo(ConstantNull.INSTANCE));
    }

    /**
     * A leaf that is not declared anywhere in the mapping behaves like any other unmapped field even when its parent is
     * a nested object: the wrap loads it from {@code _source}.
     */
    public void testUnmappedLeafUnderNestedParentLoadsFromSource() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(mapping(b -> b.startObject("item").field("type", "nested").endObject())),
            null
        );
        var defaultCtx = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var unmappedCtx = EsPhysicalOperationProviders.wrapWithUnmappedFieldContext(defaultCtx, "item.extra");
        BlockLoader blockLoader = unmappedCtx.blockLoader(
            "item.extra",
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            null,
            ByteSizeValue.ofKb(100),
            ByteSizeValue.ofKb(300)
        );
        assertThat(blockLoader, instanceOf(UnmappedKeywordBlockLoader.class));
    }

    /**
     * A field genuinely unmapped on the shard under {@code unmapped_fields="load"} loads from {@code _source} via
     * {@link UnmappedKeywordBlockLoader}, whose stored-field spec must request only that field's own source paths.
     */
    public void testUnmappedKeywordBlockLoaderRequestsOnlyItsOwnSourcePath() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(mapping(b -> b.startObject("mapped_kw").field("type", "keyword").endObject())),
            null
        );
        var defaultCtx = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var unmappedCtx = EsPhysicalOperationProviders.wrapWithUnmappedFieldContext(defaultCtx, "unmapped_kw");

        BlockLoader blockLoader = unmappedCtx.blockLoader(
            "unmapped_kw",
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            null,
            ByteSizeValue.ofKb(100),
            ByteSizeValue.ofKb(300)
        );
        assertThat(blockLoader, instanceOf(UnmappedKeywordBlockLoader.class));
        assertThat(
            "unmapped keyword loader must filter _source to its own path rather than request the whole document",
            blockLoader.rowStrideStoredFieldSpec(),
            equalTo(StoredFieldsSpec.withSourcePaths(IgnoredSourceFormat.NO_IGNORED_SOURCE, Set.of("unmapped_kw")))
        );
    }

    public void testTemporalityForMissingSetting() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(mapping(b -> b.startObject("metric_temporality").field("type", "keyword").endObject())),
            null
        );
        var shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromSingleton<>(shardContext),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        ValuesSourceReaderOperator.LoaderAndConverter loaderAndConverter = temporalityLoader(provider);
        assertThat(loaderAndConverter.loader(), equalTo(ConstantNull.INSTANCE));
    }

    public void testTemporalityHappyPath() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                tsdbSettings("metric_temporality"),
                mapping(
                    b -> b.startObject("@timestamp")
                        .field("type", "date")
                        .endObject()
                        .startObject("metric_temporality")
                        .field("type", "keyword")
                        .field("time_series_dimension", true)
                        .endObject()
                )
            ),
            null
        );
        var shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        var provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromSingleton<>(shardContext),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        ValuesSourceReaderOperator.LoaderAndConverter loaderAndConverter = temporalityLoader(provider);
        assertThat(loaderAndConverter.loader(), instanceOf(BytesRefsFromOrdsBlockLoader.class));
        ensureNoWarnings();
    }

    /**
     * Verifies that when {@code index.mapping.exclude_source_vectors} is enabled,
     * the source filter retains the original field includes from the ES|QL projection
     * instead of replacing them with an include-all filter.
     */
    public void testSourceFilterPreservesIncludesWhenVectorFieldsExcluded() throws IOException {
        var indexSettings = Settings.builder().put("index.mapping.exclude_source_vectors", true).build();
        var mapperService = createMapperService(indexSettings, mapping(b -> {
            b.startObject("text_field").field("type", "text").endObject();
            b.startObject("keyword_field").field("type", "keyword").endObject();
            b.startObject("other_field").field("type", "keyword").endObject();
            b.startObject("embedding").field("type", "dense_vector").field("dims", 3).endObject();
        }));
        var searchExecutionContext = createSearchExecutionContext(mapperService, null);

        SourceFilter filter = EsPhysicalOperationProviders.DefaultShardContext.buildSourceFilter(
            Set.of("text_field", "keyword_field"),
            searchExecutionContext.getMappingLookup(),
            searchExecutionContext.getIndexSettings()
        );

        assertNotNull("filter must not be null", filter);

        var docSource = org.elasticsearch.search.lookup.Source.fromMap(
            Map.of("text_field", "hello", "keyword_field", "world", "other_field", "extra", "embedding", List.of(1, 2, 3)),
            XContentType.JSON
        );
        var filtered = filter.filterMap(docSource);
        var result = filtered.source();

        assertThat("text_field must be present", result.containsKey("text_field"), equalTo(true));
        assertThat("keyword_field must be present", result.containsKey("keyword_field"), equalTo(true));
        assertThat("other_field must be excluded", result.containsKey("other_field"), equalTo(false));
        assertThat("embedding must be excluded", result.containsKey("embedding"), equalTo(false));
        assertThat("exactly 2 fields survive", result.size(), equalTo(2));
    }

    public void testBuildSourceFilterWithTooComplexPatternsThrowsIllegalArgument() throws IOException {
        var indexSettings = Settings.builder().put("index.mapping.exclude_source_vectors", true).build();
        var mapperService = createMapperService(indexSettings, mapping(b -> {
            b.startObject("text_field").field("type", "text").endObject();
            b.startObject("embedding").field("type", "dense_vector").field("dims", 3).endObject();
        }));
        var searchExecutionContext = createSearchExecutionContext(mapperService, null);

        Set<String> complexPaths = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            complexPaths.add("*" + randomAlphaOfLength(10) + "*");
        }

        var mappingLookup = searchExecutionContext.getMappingLookup();
        var idxSettings = searchExecutionContext.getIndexSettings();
        expectThrows(
            IllegalArgumentException.class,
            () -> EsPhysicalOperationProviders.DefaultShardContext.buildSourceFilter(complexPaths, mappingLookup, idxSettings)
        );
    }

    /** INITIAL mode, single plain field-attribute key matching the shard's primary index sort field, no competing sort pushdown. */
    public void testIsGroupKeyPrimarySortFieldHappyPath() throws IOException {
        var provider = providerWithSortedField(sortedFieldSettings("counter_id"), "counter_id");
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, counterId, esQueryExec(null));
        assertTrue(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    /**
     * A {@code STATS AVG(fn(field)) BY key} plan has an {@link EvalExec} computing the aggregated expression
     * between the {@link AggregateExec} and the {@link EsQueryExec} (e.g. a surrogate feeding SUM/COUNT). It must
     * not block the walk to the underlying {@link EsQueryExec}: {@link EvalExec} never reorders or drops rows.
     */
    public void testIsGroupKeyPrimarySortFieldSkipsIntermediateEvalExec() throws IOException {
        var provider = providerWithSortedField(sortedFieldSettings("counter_id"), "counter_id");
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        PhysicalPlan child = new EvalExec(Source.EMPTY, esQueryExec(null), List.of());
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, counterId, child);
        assertTrue(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    /** A coordinator-side (FINAL) aggregation merges intermediate state from other nodes; there is no sort guarantee to rely on. */
    public void testIsGroupKeyPrimarySortFieldDisabledForFinalMode() throws IOException {
        var provider = providerWithSortedField(sortedFieldSettings("counter_id"), "counter_id");
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.FINAL, counterId, esQueryExec(null));
        assertFalse(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    /** A competing ORDER BY pushed down as a Lucene sort means rows are not read in the index's native sort order. */
    public void testIsGroupKeyPrimarySortFieldDisabledForCompetingSortPushdown() throws IOException {
        var provider = providerWithSortedField(sortedFieldSettings("counter_id"), "counter_id");
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        List<EsQueryExec.Sort> competingSort = List.of(
            new EsQueryExec.FieldSort(counterId, Order.OrderDirection.ASC, Order.NullsPosition.LAST)
        );
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, counterId, esQueryExec(competingSort));
        assertFalse(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    /** The grouping key is not the field the index is actually sorted on. */
    public void testIsGroupKeyPrimarySortFieldDisabledForNonMatchingField() throws IOException {
        var provider = providerWithSortedField(sortedFieldSettings("counter_id"), "counter_id", "other_field");
        FieldAttribute otherField = fieldAttribute("other_field", DataType.INTEGER);
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, otherField, esQueryExec(null));
        assertFalse(provider.isGroupKeyPrimarySortField(otherField, aggregateExec));
    }

    /** No index sort configured at all. */
    public void testIsGroupKeyPrimarySortFieldDisabledWhenIndexIsNotSorted() throws IOException {
        var provider = providerWithSortedField(Settings.EMPTY, "counter_id");
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, counterId, esQueryExec(null));
        assertFalse(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    /** Decided once per fragment: if any shard contributing to it lacks the matching primary sort, the whole fragment is disabled. */
    public void testIsGroupKeyPrimarySortFieldDisabledWhenAnyShardDoesNotMatch() throws IOException {
        var sortedShard = shardContext(sortedFieldSettings("counter_id"), "counter_id");
        var unsortedShard = shardContext(Settings.EMPTY, "counter_id");
        var provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromList<>(List.of(sortedShard, unsortedShard)),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        FieldAttribute counterId = fieldAttribute("counter_id", DataType.INTEGER);
        AggregateExec aggregateExec = aggregateExec(AggregatorMode.INITIAL, counterId, esQueryExec(null));
        assertFalse(provider.isGroupKeyPrimarySortField(counterId, aggregateExec));
    }

    private static Settings sortedFieldSettings(String sortedFieldName) {
        return Settings.builder().put(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), sortedFieldName).build();
    }

    private EsPhysicalOperationProviders.DefaultShardContext shardContext(Settings indexSettings, String... integerFields)
        throws IOException {
        var mapperService = createMapperService(indexSettings, mapping(b -> {
            for (String field : integerFields) {
                b.startObject(field).field("type", "integer").endObject();
            }
        }));
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService, null);
        return new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), searchExecutionContext, AliasFilter.EMPTY);
    }

    private EsPhysicalOperationProviders providerWithSortedField(Settings indexSettings, String... integerFields) throws IOException {
        return new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromSingleton<>(shardContext(indexSettings, integerFields)),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
    }

    private static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static EsQueryExec esQueryExec(List<EsQueryExec.Sort> sorts) {
        return new EsQueryExec(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            List.of(),
            null,
            sorts,
            10,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
    }

    private static AggregateExec aggregateExec(AggregatorMode mode, Expression grouping, PhysicalPlan child) {
        return new AggregateExec(Source.EMPTY, child, List.of(grouping), List.of(), mode, List.of(), null);
    }

    private static BlockLoader blockLoader(SearchExecutionContext searchExecutionContext, String fieldName) {
        var shardContext = new EsPhysicalOperationProviders.DefaultShardContext(
            0,
            new NoOpReleasable(),
            searchExecutionContext,
            AliasFilter.EMPTY
        );
        return shardContext.blockLoader(
            fieldName,
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            null,
            ByteSizeValue.ofKb(100),
            ByteSizeValue.ofKb(300)
        );
    }

    private ValuesSourceReaderOperator.LoaderAndConverter temporalityLoader(EsPhysicalOperationProviders provider) {
        EsQueryExec queryExec = new EsQueryExec(
            Source.EMPTY,
            "test",
            IndexMode.TIME_SERIES,
            List.of(),
            null,
            null,
            10,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
        FieldExtractExec fieldExtractExec = new FieldExtractExec(
            Source.EMPTY,
            queryExec,
            List.of(new TemporalityAttribute(Source.EMPTY)),
            MappedFieldType.FieldExtractPreference.NONE
        );
        var fieldInfo = provider.extractFields(fieldExtractExec).getFirst();
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
        return fieldInfo.buildLoader().build(driverContext, 0);
    }

    private static Settings tsdbSettings(String temporalityFieldName) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_TEMPORALITY_FIELD.getKey(), temporalityFieldName)
            .build();
    }

    protected static SearchExecutionContext createMockContext() {
        Index index = new Index(randomAlphaOfLengthBetween(1, 10), "_na_");
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(
            index,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
        );
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(idxSettings, Mockito.mock(BitsetFilterCache.Listener.class));
        BiFunction<MappedFieldType, FieldDataContext, IndexFieldData<?>> indexFieldDataLookup = (fieldType, fdc) -> {
            IndexFieldData.Builder builder = fieldType.fielddataBuilder(fdc);
            return builder.build(new IndexFieldDataCache.None(), null);
        };
        MappingLookup lookup = MappingLookup.fromMapping(Mapping.EMPTY, randomFrom(IndexMode.availableModes()));
        return new SearchExecutionContext(
            0,
            0,
            idxSettings,
            bitsetFilterCache,
            indexFieldDataLookup,
            null,
            lookup,
            null,
            null,
            null,
            null,
            null,
            null,
            () -> 0,
            null,
            null,
            () -> true,
            null,
            emptyMap(),
            null,
            MapperMetrics.NOOP,
            SearchExecutionContextHelper.SHARD_SEARCH_STATS
        ) {
            @Override
            public MappedFieldType getFieldType(String name) {
                return randomFrom(
                    new KeywordFieldMapper.KeywordFieldType(name),
                    new NumberFieldMapper.NumberFieldType(name, randomFrom(NumberFieldMapper.NumberType.values()))
                );
            }

            @Override
            public NestedLookup nestedLookup() {
                return NestedLookup.EMPTY;
            }
        };
    }
}
