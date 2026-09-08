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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.SYNTHETIC_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TIMESTAMP;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ID;
import static org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdPostingsFormat.TS_ROUTING_HASH;

/**
 * Validates the fields a synthetic id is derived from. Reads check any segment that records a synthetic id; writes check when the
 * index settings say segments must have one.
 */
public final class ValidatingFieldInfosFormat extends FieldInfosFormat {

    private static final List<String> REQUIRED_FIELDS = List.of(TS_ID, TIMESTAMP, TS_ROUTING_HASH, SYNTHETIC_ID);

    private final FieldInfosFormat delegate;

    private final boolean requireSyntheticIdOnWrite;

    /**
     * @param requireSyntheticIdOnWrite whether segments written through this instance must carry a synthetic id, which the index
     *                                  settings decide
     */
    public ValidatingFieldInfosFormat(FieldInfosFormat delegate, boolean requireSyntheticIdOnWrite) {
        this.delegate = delegate;
        this.requireSyntheticIdOnWrite = requireSyntheticIdOnWrite;
    }

    /** Whether {@code fieldInfos} says the segment has a synthetic id, which is what makes the rest of the check apply. */
    private static boolean hasSyntheticId(FieldInfos fieldInfos) {
        var idFieldInfo = fieldInfos.fieldInfo(SYNTHETIC_ID);
        return idFieldInfo != null && SyntheticIdField.hasSyntheticIdAttributes(idFieldInfo.attributes());
    }

    private void ensureSyntheticIdFields(FieldInfos fieldInfos) {
        List<String> missingFields = null;
        for (String fieldName : REQUIRED_FIELDS) {
            var fi = fieldInfos.fieldInfo(fieldName);
            if (fi == null) {
                if (missingFields == null) {
                    missingFields = new ArrayList<>(REQUIRED_FIELDS.size());
                }
                missingFields.add(fieldName);
                continue;
            }

            if (SYNTHETIC_ID.equals(fi.getName())) {
                // Ensure _id has correct index options
                var idFieldInfo = fieldInfos.fieldInfo(SYNTHETIC_ID);
                if (idFieldInfo.getIndexOptions() != IndexOptions.DOCS) {
                    assert false;
                    throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] has incorrect index options");
                }
                if (SyntheticIdField.hasSyntheticIdAttributes(idFieldInfo.attributes()) == false) {
                    throw new IllegalArgumentException("Field [" + SYNTHETIC_ID + "] is not synthetic");
                }
            }
        }

        if (missingFields != null && missingFields.isEmpty() == false) {
            // A segment containing only no-op tombstones does not have the fields required
            // to synthesize the _id, but it has _soft_deletes and _tombstone fields.
            // See PerThreadIDVersionAndSeqNoLookup for a similar check.
            if (isNoOpOnlySegment(fieldInfos) == false) {
                var message = "Field(s) " + missingFields + " does not exist";
                assert false : message;
                throw new IllegalArgumentException(message);
            }
        }
    }

    /**
     * Check if this segment contains only no-op tombstones.
     * A no-op only segment has _soft_deletes and _tombstone fields with doc values.
     */
    private boolean isNoOpOnlySegment(FieldInfos fieldInfos) {
        var softDeletesField = fieldInfos.fieldInfo(Lucene.SOFT_DELETES_FIELD);
        if (softDeletesField == null) {
            return false;
        }
        var tombstoneField = fieldInfos.fieldInfo(SeqNoFieldMapper.TOMBSTONE_NAME);
        if (tombstoneField == null) {
            return false;
        }
        return softDeletesField.getDocValuesType() != DocValuesType.NONE && tombstoneField.getDocValuesType() != DocValuesType.NONE;
    }

    @Override
    public void write(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, FieldInfos fieldInfos, IOContext context)
        throws IOException {
        if (requireSyntheticIdOnWrite) {
            ensureSyntheticIdFields(fieldInfos);
        }
        delegate.write(directory, segmentInfo, segmentSuffix, fieldInfos, context);
    }

    @Override
    public FieldInfos read(Directory directory, SegmentInfo segmentInfo, String segmentSuffix, IOContext iocontext) throws IOException {
        final var fieldInfos = delegate.read(directory, segmentInfo, segmentSuffix, iocontext);
        if (hasSyntheticId(fieldInfos)) {
            ensureSyntheticIdFields(fieldInfos);
        }
        return fieldInfos;
    }
}
