/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.UnionBlock;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A union column read across shards that map its field differently. Neither the converter's own tests nor the block's can
 * see what these do: that the planner asks each shard's own mapping which leg that shard's values belong in, and that one
 * page spanning two mappings comes back as one column carrying both legs - which is the whole point of the type, and the
 * thing that would silently read one mapping's values through the other shard's loader if the arm picked wrong.
 */
public class UnionFieldExtractionTests extends MapperServiceTestCase {

    private static final String FIELD = "duration";

    /**
     * The leg order is the conflicted field's type names sorted, which the analyzer fixes once and both the block and
     * whatever renders it derive again - so it is a contract rather than an implementation detail, and spelling it out
     * here fails on a change to it rather than leaving it to surface as values appearing under the wrong type.
     */
    private static final UnionEsField UNION = new UnionEsField(FIELD, List.of(DataType.KEYWORD, DataType.LONG));
    private static final int KEYWORD_LEG = 0;
    private static final int LONG_LEG = 1;

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
    private final DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);

    public void testEachShardLoadsTheLegItsOwnMappingPutsTheFieldIn() throws IOException {
        MapperService longMapper = mapperFor("long");
        MapperService keywordMapper = mapperFor("keyword");
        // Values nothing like each other, so a leg that loaded the other shard's values could not pass for the right one.
        withLuceneIndex(longMapper, writer -> {
            writer.addDocument(doc(longMapper, "{\"duration\": 200}"));
            writer.addDocument(doc(longMapper, "{\"duration\": 201}"));
        }, longReader -> withLuceneIndex(keywordMapper, writer -> {
            writer.addDocument(doc(keywordMapper, "{\"duration\": \"a\"}"));
            writer.addDocument(doc(keywordMapper, "{\"duration\": \"b\"}"));
        }, keywordReader -> {
            List<EsPhysicalOperationProviders.ShardContext> shards = List.of(
                shard(0, longMapper, longReader),
                shard(1, keywordMapper, keywordReader)
            );
            try (Page page = read(shards, List.of(longReader, keywordReader))) {
                DocVector docs = page.<DocBlock>getBlock(0).asVector();
                Block loaded = page.getBlock(1);
                assertThat(
                    "a union field is read into a union block whatever each shard mapped it as",
                    loaded,
                    instanceOf(UnionBlock.class)
                );
                UnionBlock union = (UnionBlock) loaded;
                assertThat(union.legElementTypes(), equalTo(List.of(ElementType.BYTES_REF, ElementType.LONG)));
                assertThat(page.getPositionCount(), equalTo(4));

                Set<Long> longs = new HashSet<>();
                Set<String> keywords = new HashSet<>();
                LongBlock longLeg = (LongBlock) union.legBlock(LONG_LEG);
                BytesRefBlock keywordLeg = (BytesRefBlock) union.legBlock(KEYWORD_LEG);
                for (int p = 0; p < page.getPositionCount(); p++) {
                    int valueIndex = union.getFirstValueIndex(p);
                    switch (docs.shards().getInt(p)) {
                        case 0 -> {
                            assertThat("the long-mapped shard's values are in the long leg", union.tag(valueIndex), equalTo(LONG_LEG));
                            longs.add(longLeg.getLong(valueIndex));
                        }
                        case 1 -> {
                            assertThat(
                                "the keyword-mapped shard's values are in the keyword leg",
                                union.tag(valueIndex),
                                equalTo(KEYWORD_LEG)
                            );
                            keywords.add(keywordLeg.getBytesRef(valueIndex, new BytesRef()).utf8ToString());
                        }
                        default -> fail("unexpected shard [" + docs.shards().getInt(p) + "]");
                    }
                }
                assertThat("every value arrives unconverted", longs, equalTo(Set.of(200L, 201L)));
                assertThat("every value arrives unconverted", keywords, equalTo(Set.of("a", "b")));
            }
        }));
    }

    /**
     * A shard whose mapping gives the field a type no declared leg has room for - the mapping moved between the
     * coordinator's field-caps and execution here. Nulls, because the leg order is fixed for every node at once and a
     * leg this node invented would be a different tag everywhere else.
     */
    public void testAShardWithNoLegForItsOwnTypeLoadsNulls() throws IOException {
        MapperService booleanMapper = mapperFor("boolean");
        withLuceneIndex(
            booleanMapper,
            writer -> writer.addDocument(doc(booleanMapper, "{\"duration\": true}")),
            reader -> assertThat(
                loaderFor(List.of(shard(0, booleanMapper, reader)), 0),
                sameInstance(ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS)
            )
        );
    }

    /** A shard that does not map the field at all. The leg is declared on the coordinator's behalf; nothing here loads it. */
    public void testAShardThatDoesNotMapTheFieldLoadsNulls() throws IOException {
        MapperService otherMapper = createMapperService(mapping(b -> b.startObject("other").field("type", "long").endObject()));
        withLuceneIndex(
            otherMapper,
            writer -> writer.addDocument(doc(otherMapper, "{\"other\": 1}")),
            reader -> assertThat(
                loaderFor(List.of(shard(0, otherMapper, reader)), 0),
                sameInstance(ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS)
            )
        );
    }

    private MapperService mapperFor(String type) throws IOException {
        return createMapperService(mapping(b -> b.startObject(FIELD).field("type", type).endObject()));
    }

    private Iterable<IndexableField> doc(MapperService mapperService, String json) {
        return mapperService.documentMapper().parse(source(json)).rootDoc();
    }

    private EsPhysicalOperationProviders.ShardContext shard(int index, MapperService mapperService, DirectoryReader reader) {
        return new EsPhysicalOperationProviders.DefaultShardContext(
            index,
            new NoOpReleasable(),
            new EsqlSearchExecutionContext(createSearchExecutionContext(mapperService, newSearcher(reader)), QueryWarnings.EMIT, Set.of()),
            AliasFilter.EMPTY
        );
    }

    /** What the planner decided this shard should load for the union column, taken from the real field-extract path. */
    private ValuesSourceReaderOperator.LoaderAndConverter loaderFor(
        List<EsPhysicalOperationProviders.ShardContext> shards,
        int shardIndex
    ) {
        return fieldInfos(shards).getFirst().buildLoader().build(driverContext, shardIndex);
    }

    private List<ValuesSourceReaderOperator.FieldInfo> fieldInfos(List<EsPhysicalOperationProviders.ShardContext> shards) {
        EsPhysicalOperationProviders provider = new EsPhysicalOperationProviders(
            FoldContext.small(),
            new IndexedByShardIdFromList<>(shards),
            null,
            PlannerSettings.DEFAULTS,
            () -> 0L,
            QueryWarnings.EMIT
        );
        EsQueryExec query = new EsQueryExec(
            Source.EMPTY,
            "test",
            IndexMode.STANDARD,
            List.of(),
            null,
            null,
            10,
            List.of(new EsQueryExec.QueryBuilderAndTags(null, List.of()))
        );
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            query,
            List.of(new FieldAttribute(Source.EMPTY, null, null, FIELD, UNION)),
            MappedFieldType.FieldExtractPreference.NONE
        );
        List<ValuesSourceReaderOperator.FieldInfo> fieldInfos = provider.extractFields(extract);
        assertThat(fieldInfos, hasSize(1));
        return fieldInfos;
    }

    /**
     * One page holding every document of every shard, read through the operator the planner's own field infos configure.
     * A page spanning shards is what a union column is for, so the whole of the shard half only composes here.
     */
    private Page read(List<EsPhysicalOperationProviders.ShardContext> shards, List<DirectoryReader> readers) throws IOException {
        List<ValuesSourceReaderOperator.ShardContext> readerContexts = readers.stream()
            .map(reader -> new ValuesSourceReaderOperator.ShardContext(reader, paths -> SourceLoader.FROM_STORED_SOURCE, 0.2))
            .toList();
        List<Page> output = new ArrayList<>();
        try (
            Operator operator = new ValuesSourceReaderOperator.Factory(
                ByteSizeValue.ofGb(1),
                fieldInfos(shards),
                new IndexedByShardIdFromList<>(readerContexts),
                randomBoolean(),
                0,
                1.0,
                500,
                () -> 0L
            ).get(driverContext)
        ) {
            operator.addInput(docs(shards, readers));
            drain(operator, output);
            operator.finish();
            drain(operator, output);
        }
        assertThat("the operator maps one page to one page", output, hasSize(1));
        return output.getFirst();
    }

    private static void drain(Operator operator, List<Page> output) {
        Page page;
        while ((page = operator.getOutput()) != null) {
            output.add(page);
        }
    }

    /** Every document of every shard, in the shard-then-segment-then-doc order the operator reads in. */
    private Page docs(List<EsPhysicalOperationProviders.ShardContext> shards, List<DirectoryReader> readers) {
        List<Integer> shardOrdinals = new ArrayList<>();
        List<Integer> segments = new ArrayList<>();
        List<Integer> docIds = new ArrayList<>();
        for (int s = 0; s < readers.size(); s++) {
            for (LeafReaderContext leaf : readers.get(s).leaves()) {
                for (int d = 0; d < leaf.reader().maxDoc(); d++) {
                    shardOrdinals.add(s);
                    segments.add(leaf.ord);
                    docIds.add(d);
                }
            }
        }
        assertThat("a page spanning one shard would not test anything", docIds.size(), greaterThanOrEqualTo(readers.size()));
        return new Page(
            new DocVector(
                new IndexedByShardIdFromList<>(shards),
                intVector(shardOrdinals),
                intVector(segments),
                intVector(docIds),
                DocVector.config()
            ).asBlock()
        );
    }

    private IntVector intVector(List<Integer> values) {
        try (IntVector.Builder builder = blockFactory.newIntVectorBuilder(values.size())) {
            values.forEach(builder::appendInt);
            return builder.build();
        }
    }
}
