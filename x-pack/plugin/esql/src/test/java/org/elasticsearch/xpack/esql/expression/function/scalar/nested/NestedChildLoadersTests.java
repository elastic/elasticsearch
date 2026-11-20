/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.BlockLoaderWarnings;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

/**
 * Reads the children of real nested documents through {@link NestedChildren}, which picks and wraps the block loader of each
 * child field, so {@link NestedParentBlockLoader} and {@link NestedChildSourceBlockLoader} are driven the way a query drives
 * them. A runtime field without a script reads its values from the parent's {@code _source}, wherever it is named.
 */
public class NestedChildLoadersTests extends ComputeTestCase {
    private static final Settings SYNTHETIC = Settings.builder().put("index.mapping.source.mode", "synthetic").build();

    private static final class MapperHelper extends MapperServiceTestCase {
        MapperService create(Settings settings, String mapping) throws IOException {
            return createMapperService(settings, mapping);
        }
    }

    private final MapperHelper mapperHelper = new MapperHelper();
    private final Directory directory = newDirectory();
    private MapperService mapperService;
    private DirectoryReader reader;
    private EsPhysicalOperationProviders.DefaultShardContext shard;
    private int[] parentDocs;
    private boolean flattenedKeys;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, mapperService, directory);
    }

    public void testRuntimeKeywordOfSyntheticSourceReadsEveryValueOfTheParent() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, runtimeMapping("keyword"), List.of("""
            {"items": [{"qty": 1, "calc": "x"}, {"qty": 2, "calc": "y"}]}""", """
            {"items": [{"qty": 3, "calc": ["p", "q"]}]}"""), List.of("qty", "calc"), List.of(DataType.LONG, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, 1L, List.of("x", "y")), row(0, 2L, List.of("x", "y")), row(1, 3L, List.of("p", "q")))));
    }

    public void testRuntimeKeywordOfStoredSourceReadsTheSameValues() throws IOException {
        List<List<Object>> rows = readChildren(Settings.EMPTY, runtimeMapping("keyword"), List.of("""
            {"items": [{"qty": 1, "calc": "x"}, {"qty": 2, "calc": "y"}]}""", """
            {"items": [{"qty": 3, "calc": ["p", "q"]}]}"""), List.of("qty", "calc"), List.of(DataType.LONG, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, 1L, List.of("x", "y")), row(0, 2L, List.of("x", "y")), row(1, 3L, List.of("p", "q")))));
    }

    public void testRuntimeLongOfSyntheticSourceReadsEveryValueOfTheParent() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, runtimeMapping("long"), List.of("""
            {"items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}"""), List.of("qty", "calc"), List.of(DataType.LONG, DataType.LONG));
        assertThat(rows, equalTo(List.of(row(0, 1L, List.of(3L, 4L)), row(0, 2L, List.of(3L, 4L)))));
    }

    public void testRuntimeFieldOfOneParentDoesNotLeakIntoTheNextParent() throws IOException {
        List<List<Object>> rows = readChildren(Settings.EMPTY, runtimeMapping("long"), List.of("""
            {"items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}""", """
            {"items": [{"qty": 3, "calc": 9}]}""", """
            {"items": []}""", """
            {"items": [{"qty": 4, "calc": 7}]}"""), List.of("qty", "calc"), List.of(DataType.LONG, DataType.LONG));
        assertThat(rows, equalTo(List.of(row(0, 1L, List.of(3L, 4L)), row(0, 2L, List.of(3L, 4L)), row(1, 3L, 9L), row(3, 4L, 7L))));
    }

    public void testRuntimeFieldOfAParentWithoutTheValueIsNull() throws IOException {
        List<List<Object>> rows = readChildren(Settings.EMPTY, runtimeMapping("keyword"), List.of("""
            {"items": [{"qty": 1}]}""", """
            {"items": [{"qty": 2, "calc": "z"}]}"""), List.of("qty", "calc"), List.of(DataType.LONG, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, 1L, null), row(1, 2L, "z"))));
    }

    /** The nested object keeps its whole source, so the child's values are in the parent's rather than the child's own. */
    public void testKeywordKeptWholeReadsTheChildsObjectFromTheParentSource() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, keptWholeMapping(), List.of("""
            {"items": [{"tag": "ab"}, {"tag": "cd"}]}"""), List.of("tag"), List.of(DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "ab"), row(0, "cd"))));
    }

    public void testKeywordKeptWholeAppliesIgnoreAbove() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, keptWholeMapping(), List.of("""
            {"items": [{"tag": "ab"}, {"tag": "abcdef"}]}"""), List.of("tag"), List.of(DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "ab"), row(0, (Object) null))));
    }

    public void testKeywordKeptWholeConvertsNumbersAndBooleans() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, keptWholeMapping(), List.of("""
            {"items": [{"tag": 5}, {"tag": true}]}"""), List.of("tag"), List.of(DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "5"), row(0, "true"))));
    }

    public void testKeywordKeptWholeReadsAnArrayOfValues() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, keptWholeMapping(), List.of("""
            {"items": [{"tag": ["ab", "cd"]}, {"tag": ["ef"]}]}"""), List.of("tag"), List.of(DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, List.of("ab", "cd")), row(0, "ef"))));
    }

    public void testCopyToTargetOfSyntheticSourceReadsTheCopiedValues() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, copyToMapping("\"synthetic_source_keep\": \"all\","), List.of("""
            {"items": [{"sku": "A"}, {"sku": "B"}]}"""), List.of("sku", "search"), List.of(DataType.KEYWORD, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "A", "A"), row(0, "B", "B"))));
    }

    public void testCopyToTargetOfStoredSourceReadsTheCopiedValues() throws IOException {
        List<List<Object>> rows = readChildren(Settings.EMPTY, copyToMapping(""), List.of("""
            {"items": [{"sku": "A"}, {"sku": "B"}]}"""), List.of("sku", "search"), List.of(DataType.KEYWORD, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "A", "A"), row(0, "B", "B"))));
    }

    public void testCopyToTargetOfSyntheticSourceAppliesTheTargetsIgnoreAbove() throws IOException {
        List<List<Object>> rows = readChildren(SYNTHETIC, copyToMapping("\"synthetic_source_keep\": \"all\","), List.of("""
            {"items": [{"sku": "AB"}, {"sku": "ABCDEFGH"}]}"""), List.of("sku", "search"), List.of(DataType.KEYWORD, DataType.KEYWORD));
        assertThat(rows, equalTo(List.of(row(0, "AB", "AB"), row(0, "ABCDEFGH", null))));
    }

    public void testRuntimeKeywordOfSyntheticSourceReadsNumbersAndBooleansAsText() throws IOException {
        assertThat(runtimeKeyword(SYNTHETIC, "42"), equalTo(List.of(row(0, 1L, "42"))));
        assertThat(runtimeKeyword(SYNTHETIC, "true"), equalTo(List.of(row(0, 1L, "true"))));
        assertThat(runtimeKeyword(SYNTHETIC, "[\"a\", 1, true]"), equalTo(List.of(row(0, 1L, List.of("a", "1", "true")))));
    }

    public void testRuntimeKeywordOfStoredSourceReadsNumbersAndBooleansAsText() throws IOException {
        assertThat(runtimeKeyword(Settings.EMPTY, "42"), equalTo(List.of(row(0, 1L, "42"))));
        assertThat(runtimeKeyword(Settings.EMPTY, "true"), equalTo(List.of(row(0, 1L, "true"))));
        assertThat(runtimeKeyword(Settings.EMPTY, "[\"a\", 1, true]"), equalTo(List.of(row(0, 1L, List.of("a", "1", "true")))));
    }

    public void testRuntimeFieldIsReadOncePerParentWhereItsChildrenSpanTwoPages() throws IOException {
        for (StoredFieldsSpec spec : List.of(StoredFieldsSpec.NO_REQUIREMENTS, StoredFieldsSpec.NEEDS_SOURCE)) {
            Counted counted = readRuntimeLongOfParents(spec, List.of("""
                {"items": [{"calc": 3}, {"calc": 4}, {"calc": 5}]}"""), List.of(new int[] { 0, 1 }, new int[] { 2 }));
            assertThat(counted.values(), equalTo(Collections.nCopies(3, List.of(3L, 4L, 5L))));
            assertThat(counted.delegateReads(), equalTo(1));
            closeIndexForNextRun();
        }
    }

    public void testRuntimeFieldIsReadOncePerParentOfSeveralParents() throws IOException {
        for (StoredFieldsSpec spec : List.of(StoredFieldsSpec.NO_REQUIREMENTS, StoredFieldsSpec.NEEDS_SOURCE)) {
            Counted counted = readRuntimeLongOfParents(spec, List.of("""
                {"items": [{"calc": 3}, {"calc": 4}]}""", """
                {"items": [{"calc": 9}]}""", """
                {"items": [{"calc": 7}, {"calc": 8}, {"calc": 6}]}"""), List.of(new int[] { 0, 1, 3, 5 }, new int[] { 6, 7 }));
            assertThat(
                counted.values(),
                equalTo(List.of(List.of(3L, 4L), List.of(3L, 4L), 9L, List.of(7L, 8L, 6L), List.of(7L, 8L, 6L), List.of(7L, 8L, 6L)))
            );
            assertThat(counted.delegateReads(), equalTo(3));
            closeIndexForNextRun();
        }
    }

    public void testFlattenedKeyReadsThroughTheCastToTheColumnType() throws IOException {
        flattenedKeys = true;
        String documents = """
            {"items": [{"attrs": {"score": "42"}}, {"attrs": {"score": "not-a-number"}}]}""";
        assertThat(
            readChildren(Settings.EMPTY, flattenedMapping(), List.of(documents), List.of("attrs.score"), List.of(DataType.LONG)),
            equalTo(List.of(row(0, 42L), row(0, (Object) null)))
        );
        closeIndexForNextRun();
        assertThat(
            readChildren(Settings.EMPTY, flattenedMapping(), List.of(documents), List.of("attrs.score"), List.of(DataType.INTEGER)),
            equalTo(List.of(row(0, 42), row(0, (Object) null)))
        );
        closeIndexForNextRun();
        assertThat(
            readChildren(Settings.EMPTY, flattenedMapping(), List.of(documents), List.of("attrs.score"), List.of(DataType.KEYWORD)),
            equalTo(List.of(row(0, "42"), row(0, "not-a-number")))
        );
    }

    public void testFlattenedKeyIsNullWhereTheShardDoesNotReadKeys() throws IOException {
        for (DataType type : List.of(DataType.LONG, DataType.KEYWORD)) {
            List<List<Object>> rows = readChildren(Settings.EMPTY, flattenedMapping(), List.of("""
                {"items": [{"attrs": {"score": "42"}}]}"""), List.of("attrs.score"), List.of(type));
            assertThat(type.toString(), rows, equalTo(List.of(row(0, (Object) null))));
            closeIndexForNextRun();
        }
    }

    public void testUnsupportedChildIsNullWhereItsMapperCannotLoadBlocks() throws IOException {
        List<List<Object>> rows = readChildren(
            Settings.EMPTY,
            """
                {"_doc": {"properties": {"items": {"type": "nested", "properties": {
                  "qty": {"type": "long"},
                  "span": {"type": "integer_range"}
                }}}}}
                """,
            List.of("""
                {"items": [{"qty": 1, "span": {"gte": 1, "lte": 3}}, {"qty": 2}]}"""),
            List.of("qty", "span"),
            List.of(DataType.LONG, DataType.UNSUPPORTED)
        );
        assertThat(rows, equalTo(List.of(row(0, 1L, null), row(0, 2L, null))));
    }

    public void testReadsAllChildrenOfAParentWithMoreThanOnePage() throws IOException {
        int children = NestedChildren.PAGE_SIZE * 3;
        List<List<Object>> rows = readChildren(
            Settings.EMPTY,
            qtyMapping(),
            List.of(parentWithChildren(children), parentWithChildren(2)),
            List.of("qty"),
            List.of(DataType.LONG)
        );
        assertThat(rows.stream().filter(row -> row.get(0).equals(0)).count(), equalTo((long) children));
        assertThat(rows.stream().filter(row -> row.get(0).equals(1)).count(), equalTo(2L));
    }

    public void testStopsReadingAParentsChildrenOnceItNeedsNoMore() throws IOException {
        int children = NestedChildren.PAGE_SIZE * 3;
        List<List<Object>> rows = readChildren(
            Settings.EMPTY,
            qtyMapping(),
            List.of(parentWithChildren(children), parentWithChildren(2)),
            List.of("qty"),
            List.of(DataType.LONG),
            read -> read.isEmpty() == false
        );
        assertThat(rows.stream().filter(row -> row.get(0).equals(0)).count(), equalTo((long) NestedChildren.PAGE_SIZE));
        assertThat(rows.stream().filter(row -> row.get(0).equals(1)).count(), equalTo(2L));
    }

    private static String qtyMapping() {
        return """
            {"_doc": {"properties": {"items": {"type": "nested", "properties": {"qty": {"type": "long"}}}}}}
            """;
    }

    private static String parentWithChildren(int children) {
        StringBuilder json = new StringBuilder("{\"items\": [");
        for (int i = 0; i < children; i++) {
            json.append(i == 0 ? "" : ",").append("{\"qty\": ").append(i).append('}');
        }
        return json.append("]}").toString();
    }

    public void testDeclaredSubfieldOfAnObjectStillReads() throws IOException {
        List<List<Object>> rows = readChildren(Settings.EMPTY, """
            {"_doc": {"properties": {"items": {"type": "nested", "properties": {
              "attrs": {"properties": {"score": {"type": "long"}}}
            }}}}}
            """, List.of("""
            {"items": [{"attrs": {"score": 7}}]}"""), List.of("attrs.score"), List.of(DataType.LONG));
        assertThat(rows, equalTo(List.of(row(0, 7L))));
    }

    public void testRuntimeDateWithAFormatReadsTheSameFromStoredAndSyntheticSource() throws IOException {
        for (String calc : List.of("5", "\"5\"", "[5, 6]")) {
            List<List<Object>> stored = runtimeCalc(
                Settings.EMPTY,
                runtimeMapping("date", "\"format\": \"epoch_second\""),
                calc,
                DataType.DATETIME
            );
            List<List<Object>> synthetic = runtimeCalc(
                SYNTHETIC,
                runtimeMapping("date", "\"format\": \"epoch_second\""),
                calc,
                DataType.DATETIME
            );
            assertThat(calc, synthetic, equalTo(stored));
        }
        assertThat(
            runtimeCalc(SYNTHETIC, runtimeMapping("date", "\"format\": \"epoch_second\""), "5", DataType.DATETIME),
            equalTo(List.of(row(0, 1L, 5000L)))
        );
    }

    public void testRuntimeDateWithTheDefaultFormatReadsTheSameFromStoredAndSyntheticSource() throws IOException {
        for (String calc : List.of("\"2020-07-22T16:09:41.355Z\"", "1595434181355", "\"not a date\"")) {
            List<List<Object>> stored = runtimeCalc(Settings.EMPTY, runtimeMapping("date", ""), calc, DataType.DATETIME);
            List<List<Object>> synthetic = runtimeCalc(SYNTHETIC, runtimeMapping("date", ""), calc, DataType.DATETIME);
            assertThat(calc, synthetic, equalTo(stored));
        }
    }

    public void testRuntimeKeywordOfAnObjectAndAStringReadsTheSameFromStoredAndSyntheticSource() throws IOException {
        List<String> documents = List.of("""
            {"items": [{"qty": 1, "calc": {"raw": "v"}}, {"qty": 2, "calc": "ok"}]}""");
        List<List<Object>> stored = readChildren(
            Settings.EMPTY,
            runtimeMapping("keyword", ""),
            documents,
            List.of("qty", "calc"),
            List.of(DataType.LONG, DataType.KEYWORD)
        );
        closeIndexForNextRun();
        List<List<Object>> synthetic = readChildren(
            SYNTHETIC,
            runtimeMapping("keyword", ""),
            documents,
            List.of("qty", "calc"),
            List.of(DataType.LONG, DataType.KEYWORD)
        );
        assertThat(synthetic, equalTo(stored));
    }

    private List<List<Object>> runtimeCalc(Settings settings, String mapping, String calc, DataType type) throws IOException {
        List<List<Object>> rows = readChildren(
            settings,
            mapping,
            List.of("{\"items\": [{\"qty\": 1, \"calc\": " + calc + "}]}"),
            List.of("qty", "calc"),
            List.of(DataType.LONG, type)
        );
        closeIndexForNextRun();
        return rows;
    }

    private List<List<Object>> runtimeKeyword(Settings settings, String calc) throws IOException {
        List<List<Object>> rows = readChildren(
            settings,
            runtimeMapping("keyword"),
            List.of("{\"items\": [{\"qty\": 1, \"calc\": " + calc + "}]}"),
            List.of("qty", "calc"),
            List.of(DataType.LONG, DataType.KEYWORD)
        );
        closeIndexForNextRun();
        return rows;
    }

    private void closeIndexForNextRun() throws IOException {
        IOUtils.close(reader, mapperService);
        reader = null;
        mapperService = null;
        for (String file : directory.listAll()) {
            directory.deleteFile(file);
        }
    }

    private static String copyToMapping(String keep) {
        return Strings.format("""
            {
              "_doc": {
                "properties": {
                  "items": {
                    "type": "nested",
                    %s
                    "properties": {
                      "sku": { "type": "keyword", "copy_to": "items.search" },
                      "search": { "type": "keyword", "doc_values": false, "store": false, "ignore_above": 5 }
                    }
                  }
                }
              }
            }
            """, keep);
    }

    private static String flattenedMapping() {
        return """
            {"_doc": {"properties": {"items": {"type": "nested", "properties": {"attrs": {"type": "flattened"}}}}}}
            """;
    }

    private record Counted(int delegateReads, List<Object> values) {}

    /** Wraps {@code delegate}, counting its row reads and reporting {@code spec} as what it loads for. */
    private static final class CountingLoader implements BlockLoader {
        private final BlockLoader delegate;
        private final StoredFieldsSpec spec;
        private int reads;

        CountingLoader(BlockLoader delegate, StoredFieldsSpec spec) {
            this.delegate = delegate;
            this.spec = spec;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return delegate.builder(factory, expectedCount);
        }

        @Override
        public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
            return null;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            RowStrideReader reader = delegate.rowStrideReader(breaker, context);
            return new RowStrideReader() {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    reads++;
                    reader.read(docId, storedFields, builder);
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return reader.canReuse(startingDocID);
                }

                @Override
                public void close() {
                    reader.close();
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return spec;
        }

        @Override
        public boolean supportsOrdinals() {
            return false;
        }

        @Override
        public SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Reads {@code items.calc}, a runtime {@code long}, of the children of {@code documents} through a {@link NestedParentBlockLoader}
     * around a loader that counts its reads, with the children given to the reader in {@code pages} of child doc ids.
     */
    private Counted readRuntimeLongOfParents(StoredFieldsSpec spec, List<String> documents, List<int[]> pages) throws IOException {
        index(Settings.EMPTY, runtimeMapping("long"), documents);
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        BlockLoader real = shard.nestedChildBlockLoader(
            "items.calc",
            new BlockLoaderWarnings(driverContext, Source.EMPTY),
            PlannerSettings.DEFAULTS.blockLoaderSizeOrdinals(),
            PlannerSettings.DEFAULTS.blockLoaderSizeScript()
        );
        CountingLoader counting = new CountingLoader(real, spec);
        SearchExecutionContext context = shard.executionContext();
        BitSetProducer parents = context.bitsetFilter(Queries.newNonNestedFilter(context.indexVersionCreated()));
        BlockLoader loader = new NestedParentBlockLoader(
            counting,
            parents,
            shard::newSourceLoader,
            NestedParentBlockLoader.valueBlockFactory(blockFactory)
        );
        var infos = List.of(
            new ValuesSourceReaderOperator.FieldInfo(
                "items.calc",
                ElementType.LONG,
                false,
                (dc, s) -> ValuesSourceReaderOperator.load(loader)
            )
        );
        var readers = new IndexedByShardIdFromSingleton<>(
            new ValuesSourceReaderOperator.ShardContext(
                reader,
                sourcePaths -> shard.newNestedChildSourceLoader(),
                shard.storedFieldsSequentialProportion()
            )
        );
        List<Object> values = new ArrayList<>();
        try (
            var operator = new ValuesSourceReaderOperator.Factory(
                PlannerSettings.DEFAULTS.valuesLoadingJumboSize(),
                infos,
                readers,
                true,
                0,
                PlannerSettings.DEFAULTS.sourceReservationFactor(),
                () -> 0
            ).get(driverContext)
        ) {
            for (int[] page : pages) {
                try (DocBlock.Builder docs = DocBlock.newBlockBuilder(blockFactory, page.length)) {
                    for (int doc : page) {
                        docs.appendShard(0).appendSegment(0).appendDoc(doc);
                    }
                    operator.addInput(new Page(docs.build()));
                }
                for (Page out = operator.getOutput(); out != null; out = operator.getOutput()) {
                    for (int p = 0; p < out.getPositionCount(); p++) {
                        values.add(normalize(BlockUtils.toJavaObject(out.<Block>getBlock(1), p)));
                    }
                    out.releaseBlocks();
                }
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        return new Counted(counting.reads, values);
    }

    private static String runtimeMapping(String type) {
        return runtimeMapping(type, "");
    }

    private static String runtimeMapping(String type, String options) {
        return Strings.format("""
            {
              "_doc": {
                "runtime": { "items.calc": { "type": "%s"%s } },
                "properties": {
                  "items": {
                    "type": "nested",
                    "dynamic": false,
                    "properties": { "qty": { "type": "long" } }
                  }
                }
              }
            }
            """, type, options.isEmpty() ? "" : ", " + options);
    }

    private static String keptWholeMapping() {
        return """
            {
              "_doc": {
                "properties": {
                  "items": {
                    "type": "nested",
                    "synthetic_source_keep": "all",
                    "properties": { "tag": { "type": "keyword", "doc_values": false, "ignore_above": 4 } }
                  }
                }
              }
            }
            """;
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    /**
     * Indexes {@code documents}, each a parent with its children stored before it in one segment, and reads the children of
     * every parent as one row per child: the parent's position, then the values of {@code paths}.
     */
    private List<List<Object>> readChildren(
        Settings settings,
        String mapping,
        List<String> documents,
        List<String> paths,
        List<DataType> types
    ) throws IOException {
        return readChildren(settings, mapping, documents, paths, types, rows -> false);
    }

    /** As above, but the first parent's remaining children are skipped once {@code firstParentIsDone} holds for the rows read. */
    private List<List<Object>> readChildren(
        Settings settings,
        String mapping,
        List<String> documents,
        List<String> paths,
        List<DataType> types,
        Predicate<List<List<Object>>> firstParentIsDone
    ) throws IOException {
        index(settings, mapping, documents);
        NestedChildren.Factory factory = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            paths,
            types,
            Collections.nCopies(paths.size(), (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS,
            EsqlTestUtils.TEST_CFG
        );
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        List<List<Object>> rows = new ArrayList<>();
        try (NestedChildren children = factory.get(driverContext); DocBlock parents = parents(blockFactory, parentDocs)) {
            children.read(
                parents.asVector(),
                position -> true,
                position -> position != 0 || firstParentIsDone.test(rows) == false,
                page -> {
                    IntVector parentPositions = page.<IntBlock>getBlock(0).asVector();
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<Object> row = new ArrayList<>();
                        row.add(parentPositions.getInt(p));
                        for (int b = 1; b < page.getBlockCount(); b++) {
                            row.add(normalize(BlockUtils.toJavaObject(page.<Block>getBlock(b), p)));
                        }
                        rows.add(row);
                    }
                }
            );
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        return rows;
    }

    /** Indexes {@code documents}, each a parent with its children stored before it in one segment. */
    private void index(Settings settings, String mapping, List<String> documents) throws IOException {
        mapperService = mapperHelper.create(settings, mapping);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
        parentDocs = new int[documents.size()];
        int doc = 0;
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < documents.size(); i++) {
                ParsedDocument parsed = mapperService.documentMapper()
                    .parse(new SourceToParse(Integer.toString(i), new BytesArray(documents.get(i)), XContentType.JSON));
                writer.addDocuments(parsed.docs());
                doc += parsed.docs().size();
                parentDocs[i] = doc - 1;
            }
        }
        reader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(mapperService.index(), 0));
        assertThat(reader.leaves().size(), equalTo(1));
        SearchExecutionContext context = mapperHelper.createSearchExecutionContext(mapperService, newSearcher(reader, false));
        if (flattenedKeys) {
            context = new EsqlSearchExecutionContext(context, QueryWarnings.EMIT, Set.of(), true);
        }
        shard = new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), context, AliasFilter.EMPTY);
    }

    private static DocBlock parents(BlockFactory blockFactory, int[] parentDocs) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, parentDocs.length)) {
            for (int parentDoc : parentDocs) {
                builder.appendShard(0).appendSegment(0).appendDoc(parentDoc);
            }
            return builder.build();
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof BytesRef bytes) {
            return bytes.utf8ToString();
        }
        if (value instanceof List<?> values) {
            return values.stream().map(NestedChildLoadersTests::normalize).toList();
        }
        return value;
    }
}
