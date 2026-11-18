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
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.NoOpReleasable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static String runtimeMapping(String type) {
        return Strings.format("""
            {
              "_doc": {
                "runtime": { "items.calc": { "type": "%s" } },
                "properties": {
                  "items": {
                    "type": "nested",
                    "dynamic": false,
                    "properties": { "qty": { "type": "long" } }
                  }
                }
              }
            }
            """, type);
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
        mapperService = mapperHelper.create(settings, mapping);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setRAMBufferSizeMB(256);
        int[] parentDocs = new int[documents.size()];
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
        var shard = new EsPhysicalOperationProviders.DefaultShardContext(0, new NoOpReleasable(), context, AliasFilter.EMPTY);
        NestedChildren.Factory factory = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            paths,
            types,
            Collections.nCopies(paths.size(), (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS
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
