/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;

import java.io.IOException;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TSBulkBlockLoadingTests extends MapperServiceTestCase {

    public void testManyTSIDs() throws IOException {
        doTestManyValues(TimeSeriesIdFieldMapper.NAME, TSIDBlockLoader.TSIDs.class);
    }

    public void testManyDimensions() throws IOException {
        doTestManyValues("host_name", TSDimensionBlockLoader.TSDimensions.class);
    }

    public void doTestManyValues(String fieldName, Class<?> expectedColumnReader) throws IOException {
        final String mappings = """
                {
                    "_doc" : {
                        "properties": {
                            "@timestamp": {
                                "type": "date",
                                "ignore_malformed": false
                            },
                            "host_name": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                }
            """;
        Settings settings = indexSettings(IndexVersion.current(), 1, 1).put("index.mode", "time_series")
            .put("index.routing_path", "host_name")
            .build();
        var mapperService = createMapperService(settings, mappings);
        try (Directory directory = newDirectory()) {
            int from = 0;
            int to = 10_000;
            int uniqueTsidEvery = 200;
            IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
            iwc.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
            iwc.setIndexSort(new Sort(new SortField(fieldName, SortField.Type.STRING, false)));
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            try (IndexWriter iw = new IndexWriter(directory, iwc)) {
                for (int i = from; i < to; i++) {
                    LuceneDocument doc = new LuceneDocument();
                    int tsid = i / uniqueTsidEvery;
                    doc.add(new SortedDocValuesField(fieldName, new BytesRef(String.format(Locale.ROOT, "%04d", tsid))));
                    iw.addDocument(doc);
                }
                iw.forceMerge(1);
            }
            var mockBlockContext = mock(MappedFieldType.BlockLoaderContext.class);
            var blockLoader = mapperService.fieldType(fieldName).blockLoader(mockBlockContext);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReaderContext context = reader.leaves().get(0);
                {
                    // One big doc block
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader, instanceOf(expectedColumnReader));
                    var docBlock = TestBlock.docs(IntStream.range(from, to).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(to - from));
                    for (int i = 0; i < block.size(); i++) {
                        String actual = ((BytesRef) block.get(i)).utf8ToString();
                        int expectedTsid = i / uniqueTsidEvery;
                        assertThat(actual, equalTo(String.format(Locale.ROOT, "%04d", expectedTsid)));
                    }
                }
                {
                    // Smaller doc blocks
                    int docBlockSize = 1000;
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader, instanceOf(expectedColumnReader));
                    for (int i = from; i < to; i += docBlockSize) {
                        var docBlock = TestBlock.docs(IntStream.range(i, i + docBlockSize).toArray());
                        var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                        assertThat(block.size(), equalTo(docBlockSize));
                        for (int j = 0; j < block.size(); j++) {
                            String actual = ((BytesRef) block.get(j)).utf8ToString();
                            int expectedTsid = (i + j) / uniqueTsidEvery;
                            assertThat(actual, equalTo(String.format(Locale.ROOT, "%04d", expectedTsid)));
                        }
                    }
                }
                {
                    // One smaller doc block:
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader, instanceOf(expectedColumnReader));
                    var docBlock = TestBlock.docs(IntStream.range(1010, 2020).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(1010));
                    for (int i = 0; i < block.size(); i++) {
                        String actual = ((BytesRef) block.get(i)).utf8ToString();
                        int expectedTsid = (1010 + i) / uniqueTsidEvery;
                        assertThat(actual, equalTo(String.format(Locale.ROOT, "%04d", expectedTsid)));
                    }
                }
                {
                    // Read two tiny blocks:
                    var columnReader = blockLoader.columnAtATimeReader(context);
                    assertThat(columnReader, instanceOf(expectedColumnReader));
                    var docBlock = TestBlock.docs(IntStream.range(32, 64).toArray());
                    var block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(32));
                    for (int i = 0; i < block.size(); i++) {
                        String actual = ((BytesRef) block.get(i)).utf8ToString();
                        int expectedTsid = (32 + i) / uniqueTsidEvery;
                        assertThat(actual, equalTo(String.format(Locale.ROOT, "%04d", expectedTsid)));
                    }

                    docBlock = TestBlock.docs(IntStream.range(64, 96).toArray());
                    block = (TestBlock) columnReader.read(TestBlock.factory(), docBlock, 0);
                    assertThat(block.size(), equalTo(32));
                    for (int i = 0; i < block.size(); i++) {
                        String actual = ((BytesRef) block.get(i)).utf8ToString();
                        int expectedTsid = (64 + i) / uniqueTsidEvery;
                        assertThat(actual, equalTo(String.format(Locale.ROOT, "%04d", expectedTsid)));
                    }
                }
            }
        }
    }

}
