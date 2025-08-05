/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.TestBlock;

import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class DateBulkBlockLoadingTests extends MapperServiceTestCase {

    public void testManyValues() throws Exception {
        final String mappings = """
                    {
                        "_doc" : {
                            "properties": {
                                "@timestamp": {
                                    "type": "date",
                                    "ignore_malformed": false
                                }
                            }
                        }
                    }
                """;
        var mapperService = createMapperService(Settings.builder().put("index.mode", "logsdb").build(), mappings);
        try (Directory directory = newDirectory()) {
            int from = 0;
            int to = 10_000;
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
            iwc.setIndexSort(new Sort(new SortField("@timestamp", SortField.Type.LONG, true)));
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            try (IndexWriter iw = new IndexWriter(directory, iwc)) {
                for (long i = from; i < to; i++) {
                    LuceneDocument doc = new LuceneDocument();
                    doc.add(new NumericDocValuesField("@timestamp", i));
                    iw.addDocument(doc);
                }
                iw.forceMerge(1);
            }
            var mockBlockContext = mock(MappedFieldType.BlockLoaderContext.class);
            var blockLoader = mapperService.fieldType("@timestamp").blockLoader(mockBlockContext);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                {
                    // One big doc block
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader.getClass().getSimpleName(), equalTo("BlockAwareSingletonLongs"));
                    var docBlock = TestBlock.docs(IntStream.range(from, to).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(to - from));
                    for (int i = 0; i < block.size(); i++) {
                        assertThat(block.get(i), equalTo(to - i - 1L));
                    }
                }
                {
                    // Smaller doc blocks
                    int docBlockSize = 1000;
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader.getClass().getSimpleName(), equalTo("BlockAwareSingletonLongs"));
                    for (int i = from; i < to; i += docBlockSize) {
                        var docBlock = TestBlock.docs(IntStream.range(i, i + docBlockSize).toArray());
                        var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                        assertThat(block.size(), equalTo(docBlockSize));
                        for (int j = 0; j < block.size(); j++) {
                            long expected = to - ((long) docBlockSize * (i / docBlockSize)) - j - 1L;
                            assertThat(block.get(j), equalTo(expected));
                        }
                    }
                }
                {
                    // One smaller doc block:
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader.getClass().getSimpleName(), equalTo("BlockAwareSingletonLongs"));
                    var docBlock = TestBlock.docs(IntStream.range(1010, 2020).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(1010));
                    for (int i = 0; i < block.size(); i++) {
                        long expected = 8990 - i - 1L;
                        assertThat(block.get(i), equalTo(expected));
                    }
                }
                {
                    // Read two tiny blocks:
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader.getClass().getSimpleName(), equalTo("BlockAwareSingletonLongs"));
                    var docBlock = TestBlock.docs(IntStream.range(32, 64).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(32));
                    for (int i = 0; i < block.size(); i++) {
                        long expected = 9968 - i - 1L;
                        assertThat(block.get(i), equalTo(expected));
                    }

                    docBlock = TestBlock.docs(IntStream.range(64, 96).toArray());
                    block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(32));
                    for (int i = 0; i < block.size(); i++) {
                        long expected = 9936 - i - 1L;
                        assertThat(block.get(i), equalTo(expected));
                    }
                }
            }
        }
    }

}
