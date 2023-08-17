/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class IdLoaderTests extends ESTestCase {

    public void testSimpleSynthesizeId() throws Exception {
        var settings = indexSettings(IndexVersion.current(), 2, 1).put(IndexSettings.MODE.getKey(), "time_series")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2000-01-01T00:00:00.000Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2001-01-01T00:00:00.000Z")
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim1")
            .build();
        var indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        var routing = (IndexRouting.ExtractFromSource) IndexRouting.fromIndexMetadata(indexMetadata);
        var idLoader = IdLoader.createTsIdLoader(routing, List.of("dim1"));

        long startTime = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parseMillis("2023-01-01T00:00:00Z");
        List<Doc> docs = List.of(
            new Doc(startTime, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "xxx"))),
            new Doc(startTime + 1, List.of(new Dimension("dim1", "aaa"), new Dimension("dim2", "yyy"))),
            new Doc(startTime + 2, List.of(new Dimension("dim1", "bbb"), new Dimension("dim2", "xxx")))
        );
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = writer -> {
            for (Doc doc : docs) {
                indexDoc(routing, writer, doc);
            }
            writer.forceMerge(1);
        };
        CheckedConsumer<IndexReader, IOException> verify = indexReader -> {
            assertThat(indexReader.leaves(), hasSize(1));
            LeafReader leafReader = indexReader.leaves().get(0).reader();
            assertThat(leafReader.numDocs(), equalTo(3));
            var leaf = idLoader.leaf(null, leafReader, new int[] { 0, 1, 2 });
            assertThat(leaf.getId(0), equalTo(expectedId(routing, docs.get(0))));
            assertThat(leaf.getId(1), equalTo(expectedId(routing, docs.get(1))));
            assertThat(leaf.getId(2), equalTo(expectedId(routing, docs.get(2))));
        };
        prepareIndexReader(buildIndex, verify);
    }

    private void prepareIndexReader(
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        CheckedConsumer<IndexReader, IOException> verify
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
            Sort sort = new Sort(
                new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING, false),
                new SortedNumericSortField(DataStreamTimestampFieldMapper.DEFAULT_PATH, SortField.Type.LONG, true)
            );
            config.setIndexSort(sort);
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, config);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader indexReader = DirectoryReader.open(directory);) {
                verify.accept(indexReader);
            }
        }
    }

    private static void indexDoc(IndexRouting.ExtractFromSource routing, RandomIndexWriter iw, Doc doc) throws IOException {
        final TimeSeriesIdFieldMapper.TimeSeriesIdBuilder builder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(routing.builder());

        final List<IndexableField> fields = new ArrayList<>();
        fields.add(new SortedNumericDocValuesField(DataStreamTimestampFieldMapper.DEFAULT_PATH, doc.timestamp));
        fields.add(new LongPoint(DataStreamTimestampFieldMapper.DEFAULT_PATH, doc.timestamp));
        for (Dimension dimension : doc.dimensions) {
            if (dimension.value instanceof Number n) {
                builder.addLong(dimension.field, n.longValue());
            } else {
                builder.addString(dimension.field, dimension.value.toString());
                fields.add(new SortedSetDocValuesField(dimension.field, new BytesRef(dimension.value.toString())));
            }
        }
        BytesRef tsid = builder.build().toBytesRef();
        fields.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, tsid));
        iw.addDocument(fields);
    }

    private static String expectedId(IndexRouting.ExtractFromSource routing, Doc doc) throws IOException {
        var routingBuilder = routing.builder();
        var timeSeriesIdBuilder = new TimeSeriesIdFieldMapper.TimeSeriesIdBuilder(routingBuilder);
        for (Dimension dimension : doc.dimensions) {
            timeSeriesIdBuilder.addString(dimension.field, dimension.value.toString());
        }
        return TsidExtractingIdFieldMapper.createId(
            false,
            routingBuilder,
            timeSeriesIdBuilder.build().toBytesRef(),
            doc.timestamp,
            new byte[16]
        );
    }

    record Doc(long timestamp, List<Dimension> dimensions) {}

    record Dimension(String field, Object value) {}

}
