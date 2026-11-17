/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormatTests;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormatTests.runTestWithRandomDocs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TSDBStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        var tsdbStoredFieldsFormat = new TSDBStoredFieldsFormat(new Lucene90StoredFieldsFormat());
        return new AssertingCodec() {
            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                return tsdbStoredFieldsFormat;
            }
        };
    }

    public void testSyntheticId() throws Exception {
        runTestWithRandomDocs((writer, finalDocs) -> {
            try (var reader = DirectoryReader.open(writer)) {
                final var storedFields = reader.storedFields();

                int totalDocs = 0;
                for (var leafReaderContext : reader.leaves()) {
                    totalDocs += leafReaderContext.reader().maxDoc();
                }
                assertThat(totalDocs, equalTo(finalDocs.values().stream().mapToInt(TSDBSyntheticIdPostingsFormatTests.Doc::version).sum()));

                int docID = 0;
                final var expectedIds = finalDocs.navigableKeySet().iterator();
                while (expectedIds.hasNext()) {
                    final var expectedId = expectedIds.next();
                    for (int version = 0; version < finalDocs.get(expectedId).version(); version++) {
                        var document = storedFields.document(docID);
                        assertThat(document.getField(IdFieldMapper.NAME).binaryValue(), equalTo(expectedId));
                        docID += 1;
                    }
                }
            }
        });
    }

    /**
     * A segment without a synthetic id keeps the reader type Lucene tests for when it selects a stored fields merge strategy.
     */
    public void testSegmentWithoutSyntheticIdIsBulkMergeable() throws Exception {
        try (var directory = newDirectory(); var writer = new IndexWriter(directory, newIndexWriterConfig().setCodec(getCodec()))) {
            var document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, "1", Field.Store.YES));
            writer.addDocument(document);
            // Read through the writer: reopening from the directory resolves the codec by name via SPI.
            try (var reader = DirectoryReader.open(writer)) {
                var fieldsReader = ((CodecReader) reader.leaves().getFirst().reader()).getFieldsReader();
                assertThat(fieldsReader, instanceOf(Lucene90CompressingStoredFieldsReader.class));
                assertThat(fieldsReader.getMergeInstance(), instanceOf(Lucene90CompressingStoredFieldsReader.class));
            }
        }
    }

    public void testSegmentsWithoutASyntheticIdReader() throws Exception {
        var format = new TSDBStoredFieldsFormat(new Lucene90StoredFieldsFormat());
        var storedFields = new RecordingStoredFieldsReader();
        var reader = format.new TSDBStoredFieldsReader(storedFields, null);

        reader.checkIntegrity();
        reader.prefetch(7);
        assertThat(storedFields.integrityChecks, equalTo(1));
        assertThat(storedFields.prefetched, equalTo(7));
    }

    private static class RecordingStoredFieldsReader extends StoredFieldsReader {
        int integrityChecks = 0;
        int prefetched = -1;

        @Override
        public void document(int docID, StoredFieldVisitor visitor) {}

        @Override
        public void prefetch(int docID) {
            prefetched = docID;
        }

        @Override
        public void checkIntegrity() {
            integrityChecks += 1;
        }

        @Override
        public StoredFieldsReader clone() {
            return this;
        }

        @Override
        public void close() throws IOException {}
    }
}
