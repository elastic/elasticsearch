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
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryStringColumnReader;
import org.elasticsearch.columnar.string.StringColumnMetadata;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.openjdk.jmh.annotations.Param;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * A merge of keyword columns whose values repeat enough to earn a dictionary, written with the options a keyword field
 * ships with. Each segment's ordinals are mapped onto the merged dictionary and encoded again; {@code copyRuns} says
 * whether those of a segment's documents that land together are read a block at a time.
 *
 * <p>Whether a segment's ordinal blocks could be carried over as stored depends on its dictionary being the merged
 * one, which is the union of the segments' dictionaries; how many segments that holds for is printed when the
 * segments are written.
 *
 * <pre>
 * ./gradlew :benchmarks:run --args="ColumnarDictionaryStringMergeBenchmark"
 * ./gradlew :benchmarks:run --args="ColumnarDictionaryStringMergeBenchmark -p data=LOG_LEVEL -p sort=NONE"
 * </pre>
 */
public class ColumnarDictionaryStringMergeBenchmark extends AbstractColumnarMergeBenchmark {

    /** Values that earn a dictionary under the options a keyword field ships with. */
    @Param({ "LOG_LEVEL", "HOSTNAME" })
    private StringData data;

    /**
     * On: where a segment's documents land together, their ordinals are read a block at a time. Off: every value's
     * ordinal is asked for on its own, as before.
     */
    @Param({ "true", "false" })
    private boolean copyRuns;

    private static final FieldType BINARY = binary();

    private BytesRef[] values;

    @Override
    void generate(int docCount) {
        values = data.generate(docCount, new Random(7));
    }

    @Override
    void addField(Document doc, int i, long timestamp) {
        doc.add(new Field(FIELD, StringFormat.payload(values[i]), BINARY));
    }

    @Override
    DocValuesFormat columnarFormat() {
        return new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(StringColumnOptions.DEFAULT),
            copyRuns
        );
    }

    @Override
    void checkSegments(DirectoryReader reader) throws IOException {
        final List<DictionaryStringColumnReader> dictionaries = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            final BinaryDocValues column = leaf.reader().getBinaryDocValues(FIELD);
            if (column instanceof ColumnarStringBinaryDocValues columnar
                && columnar.reader() instanceof DictionaryStringColumnReader dictionary) {
                dictionaries.add(dictionary);
            } else {
                throw new AssertionError("expected a dictionary columnar column for [" + data + "], got " + column);
            }
        }
        // The merged dictionary is the union of the segments' own; a segment whose dictionary is the whole of it keeps
        // every ordinal as it is.
        final TreeSet<BytesRef> union = new TreeSet<>();
        long escapes = 0;
        for (DictionaryStringColumnReader dictionary : dictionaries) {
            for (int t = 0; t < dictionary.dictionarySize(); t++) {
                union.add(BytesRef.deepCopyOf(dictionary.termAt(StringColumnMetadata.Dictionary.FIRST_TERM_ORDINAL + t, new BytesRef())));
            }
            escapes += dictionary.escapeCount();
        }
        int identical = 0;
        for (DictionaryStringColumnReader dictionary : dictionaries) {
            if (dictionary.dictionarySize() == union.size() && dictionary.escapeCount() == 0) {
                identical++;
            }
        }
        System.out.println(
            "["
                + data
                + "] "
                + identical
                + " of "
                + dictionaries.size()
                + " segments hold the merged dictionary of "
                + union.size()
                + " terms; "
                + escapes
                + " escaped values"
        );
    }

    private static FieldType binary() {
        final FieldType binary = new FieldType();
        binary.setDocValuesType(DocValuesType.BINARY);
        binary.freeze();
        return binary;
    }
}
