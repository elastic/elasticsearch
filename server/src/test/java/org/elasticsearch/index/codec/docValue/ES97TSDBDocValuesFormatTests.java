/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.codec.docValue;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.tsdb.TSIDSortedDocValues;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.IndexSettings.TIME_SERIES_END_TIME;
import static org.elasticsearch.index.IndexSettings.TIME_SERIES_START_TIME;

/** Tests ES97TSDBDocValuesFormat */
public class ES97TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    @Override
    protected Codec getCodec() {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - TimeUnit.DAYS.toMillis(1);
        final Settings settings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo")
            .put(TIME_SERIES_START_TIME.getKey(), startTime)
            .put(TIME_SERIES_END_TIME.getKey(), endTime)
            .build();
        return new PerFieldMapperCodec(
            Lucene94Codec.Mode.BEST_SPEED,
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            IndexSettingsModule.newIndexSettings("test", settings)
        );
    }

    public void testSortedManyTSID() throws IOException {
        Analyzer analyzer = new MockAnalyzer(random());
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(analyzer);
        conf.setIndexSort(new Sort(new SortField(TimeSeriesIdFieldMapper.NAME, SortField.Type.STRING)));
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory, conf);

        int numIDS = random().nextInt(1, 25);
        BytesRef[] bytesRefs = new BytesRef[numIDS];

        for (int i = 0; i < numIDS; i++) {
            byte[] data = new byte[10];
            random().nextBytes(data);
            bytesRefs[i] = new BytesRef(data);
        }
        int numDocs = random().nextInt(10000);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new SortedDocValuesField(TimeSeriesIdFieldMapper.NAME, RandomPicks.randomFrom(random(), bytesRefs)));
            writer.addDocument(doc);
        }
        if (random().nextBoolean()) {
            writer.forceMerge(1);
        }
        writer.close();
        IndexReader reader = this.maybeWrapWithMergingReader(DirectoryReader.open(directory));
        for (LeafReaderContext context : reader.leaves()) {
            assertTSIDSortedDocValues(context.reader());
            assertRandomTSIDSortedDocValues(context.reader());
        }
        reader.close();
        directory.close();
    }

    private void assertTSIDSortedDocValues(LeafReader reader) throws IOException {
        SortedDocValues docValues = reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
        assertThat(docValues, Matchers.instanceOf(TSIDSortedDocValues.class));
        TSIDSortedDocValues tsid = (TSIDSortedDocValues) docValues;
        TSIDSortedDocValues wrapped = new TSIDSortedDocValues(reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME));
        wrapped.nextDoc();
        tsid.nextDoc();
        assertEquals(wrapped.docID(), tsid.docID());
        int count = 0;
        int prevOrd = -1;
        do {
            assertEquals(wrapped.ordValue(), tsid.ordValue());
            assertEquals(prevOrd + 1, tsid.ordValue());
            prevOrd = wrapped.ordValue();
            wrapped.advanceOrd();
            tsid.advanceOrd();
            assertEquals(wrapped.docID(), tsid.docID());
            count++;
        } while (wrapped.docID() != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(wrapped.getValueCount(), tsid.getValueCount());
        assertEquals(wrapped.getValueCount(), count);
    }

    private void assertRandomTSIDSortedDocValues(LeafReader reader) throws IOException {
        SortedDocValues docValues = reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME);
        assertThat(docValues, Matchers.instanceOf(TSIDSortedDocValues.class));
        TSIDSortedDocValues tsid = (TSIDSortedDocValues) docValues;
        TSIDSortedDocValues wrapped = new TSIDSortedDocValues(reader.getSortedDocValues(TimeSeriesIdFieldMapper.NAME));
        wrapped.nextDoc();
        tsid.nextDoc();
        assertEquals(wrapped.docID(), tsid.docID());
        do {
            assertEquals(wrapped.ordValue(), tsid.ordValue());
            int steps = random().nextInt(100);
            for (int i = 0; i < steps; i++) {
                wrapped.nextDoc();
                tsid.nextDoc();
                if (tsid.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                    break;
                }
            }
            assertEquals(wrapped.docID(), tsid.docID());
            wrapped.advanceOrd();
            tsid.advanceOrd();
            assertEquals(wrapped.docID(), tsid.docID());
        } while (wrapped.docID() != DocIdSetIterator.NO_MORE_DOCS);
    }
}
