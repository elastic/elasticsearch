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
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractBytesRefsFromOrdsBlockLoader;
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
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
            PlannerSettings.DEFAULTS
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
        var unmappedCtx = EsPhysicalOperationProviders.wrapWithUnmappedFieldContext(
            defaultCtx,
            new PotentiallyUnmappedKeywordEsField("resource.attributes.host.name")
        );

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
            PlannerSettings.DEFAULTS
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
            PlannerSettings.DEFAULTS
        );
        ValuesSourceReaderOperator.LoaderAndConverter loaderAndConverter = temporalityLoader(provider);
        assertThat(loaderAndConverter.loader(), instanceOf(AbstractBytesRefsFromOrdsBlockLoader.class));
        ensureNoWarnings();
    }

    public void testTemporalityWithMissingField() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                tsdbSettings("missing_temporality"),
                mapping(
                    b -> b.startObject("@timestamp")
                        .field("type", "date")
                        .endObject()
                        .startObject("host")
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
            PlannerSettings.DEFAULTS
        );
        assertThat(temporalityLoader(provider).loader(), equalTo(ConstantNull.INSTANCE));
        ensureNoWarnings();
    }

    public void testTemporalityFieldMustBeKeyword() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                tsdbSettings("metric_temporality"),
                mapping(
                    b -> b.startObject("@timestamp")
                        .field("type", "date")
                        .endObject()
                        .startObject("metric_temporality")
                        .field("type", "long")
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
            PlannerSettings.DEFAULTS
        );
        assertThat(temporalityLoader(provider).loader(), equalTo(ConstantNull.INSTANCE));
        assertWarnings(
            "Line -1:-1: warnings during evaluation of []. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: configured temporality field [metric_temporality] has type [long], expected "
                + "[keyword]; assuming default temporality for all values"
        );
    }

    public void testTemporalityFieldMustBeDimension() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(
            createMapperService(
                tsdbSettings("metric_temporality"),
                mapping(
                    b -> b.startObject("@timestamp")
                        .field("type", "date")
                        .endObject()
                        .startObject("metric_temporality")
                        .field("type", "keyword")
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
            PlannerSettings.DEFAULTS
        );
        assertThat(temporalityLoader(provider).loader(), equalTo(ConstantNull.INSTANCE));
        assertWarnings(
            "Line -1:-1: warnings during evaluation of []. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: configured temporality field [metric_temporality] must be a time-series "
                + "dimension; assuming default temporality for all values"
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
        return fieldInfo.buildLoader().build(DriverContext.WarningsMode.COLLECT, 0);
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
        MappingLookup lookup = MappingLookup.fromMapping(Mapping.EMPTY, randomFrom(IndexMode.values()));
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
