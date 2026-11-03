/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.mapper.SyntheticIdField;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH;

public class TSDBSyntheticIdStoredFieldsReaderTests extends ESTestCase {

    public void testCheckIntegrityChecksTheDocValues() throws Exception {
        try (Directory directory = newDirectory()) {
            var producer = new RecordingDocValuesProducer();
            var reader = reader(directory, producer);

            reader.checkIntegrity();

            assertEquals("checkIntegrity must reach the doc values the synthetic id is built from", 1, producer.integrityChecks);
        }
    }

    public void testCloneSharesTheDocValuesProducer() throws Exception {
        try (Directory directory = newDirectory()) {
            var producer = new RecordingDocValuesProducer();
            var reader = reader(directory, producer);

            var clone = reader.clone();

            assertEquals("a clone reads through the same producer, not a merge instance", 0, producer.mergeInstances);
            clone.close();
            assertEquals("closing a clone must leave the shared producer open", 0, producer.closes);

            reader.close();
            assertEquals(1, producer.closes);
        }
    }

    public void testMergeInstanceReadsThroughTheMergeInstance() throws Exception {
        try (Directory directory = newDirectory()) {
            var producer = new RecordingDocValuesProducer();
            var reader = reader(directory, producer);

            var mergeInstance = reader.getMergeInstance();

            assertEquals(1, producer.mergeInstances);
            mergeInstance.close();
            assertEquals("closing a merge instance must leave the producer this reader owns open", 0, producer.closes);
        }
    }

    private static TSDBSyntheticIdStoredFieldsReader reader(Directory directory, DocValuesProducer producer) {
        var fieldInfos = syntheticIdFieldInfos();
        return new TSDBSyntheticIdStoredFieldsReader(
            directory,
            segmentInfo(directory),
            fieldInfos,
            IOContext.DEFAULT,
            producer,
            fieldInfos.fieldInfo(SYNTHETIC_ID)
        );
    }

    private static SegmentInfo segmentInfo(Directory directory) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            1,
            false,
            false,
            new Lucene104Codec(),
            Map.of(),
            StringHelper.randomId(),
            new HashMap<>(),
            null
        );
    }

    private static FieldInfos syntheticIdFieldInfos() {
        List<FieldInfo> fields = new ArrayList<>();
        fields.add(field(SYNTHETIC_ID, 0, syntheticIdAttributes()));
        fields.add(field(TS_ID, 1, Map.of()));
        fields.add(field(TIMESTAMP, 2, Map.of()));
        fields.add(field(TS_ROUTING_HASH, 3, Map.of()));
        return new FieldInfos(fields.toArray(new FieldInfo[0]));
    }

    private static Map<String, String> syntheticIdAttributes() {
        Map<String, String> attributes = Map.of(
            SyntheticIdField.class.getSimpleName() + ".enabled",
            Boolean.TRUE.toString(),
            PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY,
            TSDBSyntheticIdPostingsFormat.FORMAT_NAME,
            PerFieldPostingsFormat.PER_FIELD_SUFFIX_KEY,
            TSDBSyntheticIdPostingsFormat.SUFFIX
        );
        assertTrue("test builds attributes the predicate no longer recognises", SyntheticIdField.hasSyntheticIdAttributes(attributes));
        return attributes;
    }

    private static FieldInfo field(String name, int number, Map<String, String> attributes) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            IndexOptions.DOCS,
            DocValuesType.SORTED_NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            attributes,
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    private static class RecordingDocValuesProducer extends DocValuesProducer {
        int integrityChecks = 0;
        int mergeInstances = 0;
        int closes = 0;

        @Override
        public NumericDocValues getNumeric(FieldInfo field) {
            return null;
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) {
            return null;
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) {
            return null;
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
            return null;
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) {
            return null;
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) {
            return null;
        }

        @Override
        public DocValuesProducer getMergeInstance() {
            mergeInstances += 1;
            return this;
        }

        @Override
        public void checkIntegrity() {
            integrityChecks += 1;
        }

        @Override
        public void close() {
            closes += 1;
        }
    }
}
