/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;

/**
 * A merge of a numeric column, whose blocks are decoded and encoded again through the column's pipeline.
 *
 * <p>{@link Workload#TIMESTAMP} holds each document's own {@code @timestamp}, so under a sort on it the merged column
 * is in order; the other workloads are values that do not follow the sort. Each is written through the pipeline
 * {@link NumericFormat#selectPipeline} picks for it, so a double gauge goes through ALP.
 *
 * <pre>
 * ./gradlew :benchmarks:run --args="ColumnarNumericMergeBenchmark"
 * ./gradlew :benchmarks:run --args="ColumnarNumericMergeBenchmark -p workload=DOUBLE_GAUGE -p sort=NONE"
 * </pre>
 */
public class ColumnarNumericMergeBenchmark extends AbstractColumnarMergeBenchmark {

    /** What the column holds. */
    public enum Workload {
        TIMESTAMP(null, "MONOTONIC_TIMESTAMPS", ColumnarFieldType.LONG),
        GAUGE("GAUGE", "GAUGE", ColumnarFieldType.LONG),
        DOUBLE_GAUGE("DOUBLE_GAUGE", "DOUBLE_GAUGE", ColumnarFieldType.DOUBLE);

        /** The {@link NumericData} shape the values are drawn from, or null for the document's timestamp. */
        private final String data;
        /** The workload {@link NumericFormat#selectPipeline} chooses the pipeline by. */
        private final String pipeline;
        private final ColumnarFieldType type;

        Workload(String data, String pipeline, ColumnarFieldType type) {
            this.data = data;
            this.pipeline = pipeline;
            this.type = type;
        }
    }

    @Param({ "TIMESTAMP", "GAUGE", "DOUBLE_GAUGE" })
    private Workload workload;

    private static final FieldType BINARY = binary();

    private final BytesRefBuilder scratch = new BytesRefBuilder();
    private final long[] one = new long[1];
    private long[] values;

    @Override
    void generate(int docCount) {
        values = workload.data == null ? null : NumericData.generate(workload.data, docCount);
    }

    @Override
    void addField(Document doc, int i, long timestamp) {
        one[0] = values == null ? timestamp : values[i];
        doc.add(new Field(FIELD, BytesRef.deepCopyOf(NumericBinaryPayload.encode(one, 1, scratch)), BINARY));
    }

    @Override
    DocValuesFormat columnarFormat() {
        return new ColumNARDocValuesFormat(
            (fieldName, type) -> blockSize -> NumericFormat.selectPipeline(workload.pipeline, blockSize),
            field -> workload.type,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(StringColumnOptions.DEFAULT)
        );
    }

    @Override
    void checkSegments(DirectoryReader reader) throws IOException {
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues column = leaf.reader().getBinaryDocValues(FIELD);
            if ((column instanceof ColumnarNumericBinaryDocValues) == false) {
                throw new AssertionError("expected a numeric columnar column for [" + workload + "], got " + column);
            }
        }
    }

    private static FieldType binary() {
        final FieldType binary = new FieldType();
        binary.setDocValuesType(DocValuesType.BINARY);
        binary.freeze();
        return binary;
    }
}
