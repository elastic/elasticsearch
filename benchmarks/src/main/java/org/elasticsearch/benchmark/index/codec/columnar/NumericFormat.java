/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarNumericRangeQuery;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.numeric.NumericColumnValues;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.lucene.queries.SortedNumericDocValuesRangeQuery;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A numeric doc-values format under comparison, abstracting the codec, how a value is indexed,
 * the range query, and the sequential decode path. For {@link #COLUMNAR}, the pipeline is chosen
 * automatically from the workload name via {@link #codec(String)}, so each benchmark only needs
 * three format variants to cover the comparison: Lucene baseline, ES95, and ColumNAR.
 */
public enum NumericFormat {

    LUCENE,
    ES819,
    ES95,
    COLUMNAR;

    static final String FIELD = "value";

    private static final FieldType COLUMNAR_FIELD_TYPE = columnarFieldType();

    /**
     * Returns the codec for this format. For {@link #COLUMNAR}, the pipeline is selected from
     * the workload name so each data shape gets the encoding most appropriate for it.
     */
    Codec codec(String workload) {
        final DocValuesFormat dv = switch (this) {
            case LUCENE -> new Lucene90DocValuesFormat();
            case ES819 -> new ES819TSDBDocValuesFormat();
            case ES95 -> new ES95TSDBDocValuesFormat();
            case COLUMNAR -> new ColumNARDocValuesFormat((f, t, bs) -> selectPipeline(workload, bs));
        };
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dv;
            }
        };
    }

    void addField(Document doc, String field, long value, BytesRefBuilder builder) {
        switch (this) {
            case LUCENE, ES819, ES95 -> doc.add(SortedNumericDocValuesField.indexedField(field, value));
            case COLUMNAR -> doc.add(
                new Field(field, BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder)), COLUMNAR_FIELD_TYPE)
            );
        }
    }

    Query rangeQuery(String field, long lower, long upper) {
        return switch (this) {
            case LUCENE, ES819, ES95 -> new SortedNumericDocValuesRangeQuery(field, lower, upper);
            case COLUMNAR -> new ColumnarNumericRangeQuery(field, lower, upper);
        };
    }

    void readAll(LeafReader leafReader, String field, Blackhole bh) throws IOException {
        switch (this) {
            case LUCENE, ES819, ES95 -> {
                final SortedNumericDocValues dv = leafReader.getSortedNumericDocValues(field);
                if (dv == null) {
                    return;
                }
                while (dv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    for (int i = 0; i < dv.docValueCount(); i++) {
                        bh.consume(dv.nextValue());
                    }
                }
            }
            case COLUMNAR -> {
                final BinaryDocValues raw = leafReader.getBinaryDocValues(field);
                if (raw == null) {
                    return;
                }
                final NumericColumnValues values = ((ColumnarNumericBinaryDocValues) raw).directValues();
                while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    for (int i = 0; i < values.valueCount(); i++) {
                        bh.consume(values.nextValue());
                    }
                }
            }
        }
    }

    /**
     * Writes {@code values} into a single force-merged segment under a temp {@link FSDirectory} and
     * returns that directory. Callers open a {@link org.apache.lucene.index.DirectoryReader} on the
     * result and are responsible for closing both the reader and the directory.
     */
    Directory buildSegment(String field, String workload, long[] values, String tempDirPrefix) throws IOException {
        final Path tempPath = Files.createTempDirectory(tempDirPrefix);
        final FSDirectory fsDir = FSDirectory.open(tempPath);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec(workload));
        final BytesRefBuilder builder = new BytesRefBuilder();
        try (IndexWriter writer = new IndexWriter(fsDir, config)) {
            for (long value : values) {
                final Document doc = new Document();
                addField(doc, field, value, builder);
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return new FilterDirectory(fsDir) {
            @Override
            public void close() throws IOException {
                super.close();
                IOUtils.rm(tempPath);
            }
        };
    }

    static NumericPipeline selectPipeline(String workload, int blockSize) {
        return switch (workload) {
            case "MONOTONIC_TIMESTAMPS", "TSDB_SPLIT" -> NumericPipeline.monotonicLongPipeline(blockSize);
            case "DOUBLE_GAUGE", "SENSOR_DOUBLES" -> NumericPipeline.doubleGaugePipeline(blockSize);
            case "DOUBLE_COUNTER" -> NumericPipeline.doubleCounterPipeline(blockSize);
            default -> NumericPipeline.defaultPipeline(blockSize);
        };
    }

    private static FieldType columnarFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.LONG.name());
        type.freeze();
        return type;
    }
}
