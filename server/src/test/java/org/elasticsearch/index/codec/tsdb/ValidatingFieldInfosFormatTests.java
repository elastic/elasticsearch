/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.mapper.SyntheticIdField;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH;

public class ValidatingFieldInfosFormatTests extends ESTestCase {

    private static FieldInfo field(String name, int number, IndexOptions indexOptions, Map<String, String> attributes) {
        return new FieldInfo(
            name,
            number,
            false,
            false,
            false,
            indexOptions,
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

    private static Map<String, String> syntheticIdAttributes() {
        // Built the way the mapper builds them; asserted against the predicate so this fails if the keys move.
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

    /** {@code _id} says the segment has a synthetic id, but the fields it is derived from are absent. */
    private static FieldInfos syntheticIdWithMissingFields() {
        return new FieldInfos(new FieldInfo[] { field(SYNTHETIC_ID, 0, IndexOptions.DOCS, syntheticIdAttributes()) });
    }

    private static FieldInfos syntheticIdComplete() {
        List<FieldInfo> fields = new ArrayList<>();
        fields.add(field(SYNTHETIC_ID, 0, IndexOptions.DOCS, syntheticIdAttributes()));
        fields.add(field(TS_ID, 1, IndexOptions.NONE, Map.of()));
        fields.add(field(TIMESTAMP, 2, IndexOptions.NONE, Map.of()));
        fields.add(field(TS_ROUTING_HASH, 3, IndexOptions.NONE, Map.of()));
        return new FieldInfos(fields.toArray(new FieldInfo[0]));
    }

    private static FieldInfos ordinarySegment() {
        return new FieldInfos(new FieldInfo[] { field("a_field", 0, IndexOptions.DOCS, Map.of()) });
    }

    private static FieldInfosFormat formatReturning(FieldInfos fieldInfos, boolean requireSyntheticIdOnWrite) {
        FieldInfosFormat delegate = new FieldInfosFormat() {
            @Override
            public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext context) {
                return fieldInfos;
            }

            @Override
            public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos infos, IOContext context) {}
        };
        return new ValidatingFieldInfosFormat(delegate, requireSyntheticIdOnWrite);
    }

    public void testReadRejectsASegmentThatClaimsASyntheticIdWithoutTheFieldsBehindIt() {
        var format = formatReturning(syntheticIdWithMissingFields(), false);
        // Tests run with assertions on, so the guard trips before the exception it would otherwise throw.
        var e = expectThrows(AssertionError.class, () -> format.read(null, null, "", IOContext.DEFAULT));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("does not exist"));
    }

    public void testReadAcceptsACompleteSyntheticIdSegment() throws Exception {
        assertNotNull(formatReturning(syntheticIdComplete(), false).read(null, null, "", IOContext.DEFAULT));
    }

    public void testReadIgnoresASegmentWithoutASyntheticId() throws Exception {
        // Nothing to check: an ordinary segment is not required to carry the fields a synthetic id is built from.
        assertNotNull(formatReturning(ordinarySegment(), false).read(null, null, "", IOContext.DEFAULT));
    }

    public void testWriteRejectsMissingFieldsWhenTheIndexRequiresASyntheticId() {
        var format = formatReturning(ordinarySegment(), true);
        expectThrows(AssertionError.class, () -> format.write(null, null, "", ordinarySegment(), IOContext.DEFAULT));
    }

    public void testWriteAcceptsAnOrdinarySegmentWhenTheIndexDoesNotRequireOne() throws Exception {
        formatReturning(ordinarySegment(), false).write(null, null, "", ordinarySegment(), IOContext.DEFAULT);
    }
}
