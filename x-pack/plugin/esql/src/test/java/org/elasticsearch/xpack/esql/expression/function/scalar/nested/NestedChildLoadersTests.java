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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DocBlock;
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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.BlockLoaderWarnings;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Reads the children of real nested documents through {@link NestedChildren}, which picks and wraps the block loader of each
 * child field, so {@link NestedChildSourceBlockLoader} is driven the way a query drives it. A runtime field without a script reads
 * its values from the parent's {@code _source}, wherever it is named, and is read with the parent documents rather than the children.
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
        List<Object> values = readParents(SYNTHETIC, runtimeMapping("keyword"), List.of("""
            {"items": [{"qty": 1, "calc": "x"}, {"qty": 2, "calc": "y"}]}""", """
            {"items": [{"qty": 3, "calc": ["p", "q"]}]}"""), DataType.KEYWORD);
        assertThat(values, equalTo(List.of(List.of("x", "y"), List.of("p", "q"))));
    }

    public void testRuntimeKeywordOfStoredSourceReadsTheSameValues() throws IOException {
        List<Object> values = readParents(Settings.EMPTY, runtimeMapping("keyword"), List.of("""
            {"items": [{"qty": 1, "calc": "x"}, {"qty": 2, "calc": "y"}]}""", """
            {"items": [{"qty": 3, "calc": ["p", "q"]}]}"""), DataType.KEYWORD);
        assertThat(values, equalTo(List.of(List.of("x", "y"), List.of("p", "q"))));
    }

    public void testRuntimeLongOfSyntheticSourceReadsEveryValueOfTheParent() throws IOException {
        List<Object> values = readParents(SYNTHETIC, runtimeMapping("long"), List.of("""
            {"items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}"""), DataType.LONG);
        assertThat(values, equalTo(List.of(List.of(3L, 4L))));
    }

    public void testRuntimeFieldOfOneParentDoesNotLeakIntoTheNextParent() throws IOException {
        for (Settings settings : List.of(Settings.EMPTY, SYNTHETIC)) {
            List<Object> values = readParents(settings, runtimeMapping("long"), List.of("""
                {"items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}""", """
                {"items": [{"qty": 3, "calc": 9}]}""", """
                {"items": []}""", """
                {"items": [{"qty": 4, "calc": 7}]}"""), DataType.LONG);
            assertThat(values, equalTo(Arrays.asList(List.of(3L, 4L), 9L, null, 7L)));
            closeIndexForNextRun();
        }
    }

    public void testRuntimeFieldOfAParentWithoutTheValueIsNull() throws IOException {
        for (Settings settings : List.of(Settings.EMPTY, SYNTHETIC)) {
            List<Object> values = readParents(settings, runtimeMapping("keyword"), List.of("""
                {"items": [{"qty": 1}]}""", """
                {"items": [{"qty": 2, "calc": "z"}]}"""), DataType.KEYWORD);
            assertThat(values, equalTo(Arrays.asList(null, "z")));
            closeIndexForNextRun();
        }
    }

    public void testRuntimeFieldIsNoChildOfTheNestedObject() throws IOException {
        for (Settings settings : List.of(Settings.EMPTY, SYNTHETIC)) {
            List<List<Object>> rows = readChildren(
                settings,
                runtimeMapping("long"),
                List.of("""
                    {"items": [{"qty": 1, "calc": 3}, {"qty": 2, "calc": 4}]}"""),
                List.of("qty", "calc"),
                List.of(DataType.LONG, DataType.LONG)
            );
            assertThat(rows, equalTo(List.of(row(0, 1L, null), row(0, 2L, null))));
            closeIndexForNextRun();
        }
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
        assertThat(runtimeKeyword(SYNTHETIC, "42"), equalTo(List.of("42")));
        assertThat(runtimeKeyword(SYNTHETIC, "true"), equalTo(List.of("true")));
        assertThat(runtimeKeyword(SYNTHETIC, "[\"a\", 1, true]"), equalTo(List.of(List.of("a", "1", "true"))));
    }

    public void testRuntimeKeywordOfStoredSourceReadsNumbersAndBooleansAsText() throws IOException {
        assertThat(runtimeKeyword(Settings.EMPTY, "42"), equalTo(List.of("42")));
        assertThat(runtimeKeyword(Settings.EMPTY, "true"), equalTo(List.of("true")));
        assertThat(runtimeKeyword(Settings.EMPTY, "[\"a\", 1, true]"), equalTo(List.of(List.of("a", "1", "true"))));
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
            List<Object> stored = runtimeCalc(
                Settings.EMPTY,
                runtimeMapping("date", "\"format\": \"epoch_second\""),
                calc,
                DataType.DATETIME
            );
            List<Object> synthetic = runtimeCalc(
                SYNTHETIC,
                runtimeMapping("date", "\"format\": \"epoch_second\""),
                calc,
                DataType.DATETIME
            );
            assertThat(calc, synthetic, equalTo(stored));
        }
        assertThat(
            runtimeCalc(SYNTHETIC, runtimeMapping("date", "\"format\": \"epoch_second\""), "5", DataType.DATETIME),
            equalTo(List.of(5000L))
        );
    }

    public void testRuntimeDateWithTheDefaultFormatReadsTheSameFromStoredAndSyntheticSource() throws IOException {
        for (String calc : List.of("\"2020-07-22T16:09:41.355Z\"", "1595434181355", "\"not a date\"")) {
            List<Object> stored = runtimeCalc(Settings.EMPTY, runtimeMapping("date", ""), calc, DataType.DATETIME);
            List<Object> synthetic = runtimeCalc(SYNTHETIC, runtimeMapping("date", ""), calc, DataType.DATETIME);
            assertThat(calc, synthetic, equalTo(stored));
        }
    }

    public void testRuntimeKeywordOfAnObjectAndAStringReadsTheSameFromStoredAndSyntheticSource() throws IOException {
        List<String> documents = List.of("""
            {"items": [{"qty": 1, "calc": {"raw": "v"}}, {"qty": 2, "calc": "ok"}]}""");
        List<Object> stored = readParents(Settings.EMPTY, runtimeMapping("keyword", ""), documents, DataType.KEYWORD);
        closeIndexForNextRun();
        List<Object> synthetic = readParents(SYNTHETIC, runtimeMapping("keyword", ""), documents, DataType.KEYWORD);
        assertThat(synthetic, equalTo(stored));
    }

    private List<Object> runtimeCalc(Settings settings, String mapping, String calc, DataType type) throws IOException {
        List<Object> values = readParents(settings, mapping, List.of("{\"items\": [{\"qty\": 1, \"calc\": " + calc + "}]}"), type);
        closeIndexForNextRun();
        return values;
    }

    private List<Object> runtimeKeyword(Settings settings, String calc) throws IOException {
        List<Object> values = readParents(
            settings,
            runtimeMapping("keyword"),
            List.of("{\"items\": [{\"qty\": 1, \"calc\": " + calc + "}]}"),
            DataType.KEYWORD
        );
        closeIndexForNextRun();
        return values;
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
            children.read(parents.asVector(), position -> true, page -> {
                IntVector parentPositions = page.<IntBlock>getBlock(0).asVector();
                for (int p = 0; p < page.getPositionCount(); p++) {
                    List<Object> row = new ArrayList<>();
                    row.add(parentPositions.getInt(p));
                    for (int b = 1; b < page.getBlockCount(); b++) {
                        row.add(normalize(BlockUtils.toJavaObject(page.<Block>getBlock(b), p)));
                    }
                    rows.add(row);
                }
            });
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        return rows;
    }

    /** Indexes {@code documents} and reads {@code items.calc} of every parent with the loader of a top-level field. */
    private List<Object> readParents(Settings settings, String mapping, List<String> documents, DataType type) throws IOException {
        index(settings, mapping, documents);
        BlockFactory blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        BlockLoader loader = shard.blockLoader(
            "items.calc",
            false,
            MappedFieldType.FieldExtractPreference.NONE,
            null,
            new BlockLoaderWarnings(driverContext, Source.EMPTY),
            PlannerSettings.DEFAULTS.blockLoaderSizeOrdinals(),
            PlannerSettings.DEFAULTS.blockLoaderSizeScript()
        );
        var infos = List.of(
            new ValuesSourceReaderOperator.FieldInfo(
                "items.calc",
                PlannerUtils.toElementType(type),
                false,
                (dc, s) -> ValuesSourceReaderOperator.load(loader)
            )
        );
        var readers = new IndexedByShardIdFromSingleton<>(
            new ValuesSourceReaderOperator.ShardContext(reader, shard::newSourceLoader, shard.storedFieldsSequentialProportion())
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
            operator.addInput(new Page(parents(blockFactory, parentDocs)));
            for (Page out = operator.getOutput(); out != null; out = operator.getOutput()) {
                for (int p = 0; p < out.getPositionCount(); p++) {
                    values.add(normalize(BlockUtils.toJavaObject(out.<Block>getBlock(1), p)));
                }
                out.releaseBlocks();
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        return values;
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
