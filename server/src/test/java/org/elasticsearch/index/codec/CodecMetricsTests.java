/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.CodecMetrics.Format;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.io.IOException;

import static org.hamcrest.Matchers.empty;

public class CodecMetricsTests extends ESTestCase {

    public void testAlreadyClosedIsNotCounted() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        Exception closed = new AlreadyClosedException("closed");
        new CodecMetrics(registry).onFailure(
            randomFrom(Format.values()),
            null,
            null,
            randomBoolean() ? closed : new RemoteTransportException("wrapped", closed)
        );
        assertThat(registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, CodecMetrics.CODEC_FAILURE_TOTAL), empty());
    }

    /**
     * Postings, doc values and vectors are named from the format the per-field dispatcher recorded on the field, codec-wide formats from the
     * codec. The class name is what makes the value unambiguous: {@code Lucene90} is the SPI name of both a doc values and a postings format.
     */
    public void testFormatNameResolvesConcreteFormat() throws IOException {
        Codec codec = new Lucene104Codec();
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(codec))) {
                Document doc = new Document();
                doc.add(new TextField("text", randomAlphaOfLengthBetween(1, 20), Field.Store.NO));
                doc.add(new NumericDocValuesField("dv", randomLong()));
                doc.add(new KnnFloatVectorField("vector", new float[] { randomFloat(), randomFloat() }));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                FieldInfos fieldInfos = reader.leaves().get(0).reader().getFieldInfos();
                assertEquals("Lucene104PostingsFormat", Format.POSTINGS.formatName(codec, fieldInfos.fieldInfo("text")));
                assertEquals("Lucene90DocValuesFormat", Format.DOC_VALUES.formatName(codec, fieldInfos.fieldInfo("dv")));
                assertEquals("Lucene99HnswVectorsFormat", Format.KNN_VECTORS.formatName(codec, fieldInfos.fieldInfo("vector")));
                // A field the format never wrote carries no format name for it.
                assertEquals(Format.DOC_VALUES.label(), Format.DOC_VALUES.formatName(codec, fieldInfos.fieldInfo("text")));
                assertEquals("Lucene90StoredFieldsFormat", Format.STORED_FIELDS.formatName(codec, null));
                assertEquals("Lucene90PointsFormat", Format.POINTS.formatName(codec, null));
                assertEquals("Lucene90NormsFormat", Format.NORMS.formatName(codec, null));
            }
        }
    }
}
