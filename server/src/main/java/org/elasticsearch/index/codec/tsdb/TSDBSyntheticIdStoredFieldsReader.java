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
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.SyntheticIdField;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/*
 * Allows to access synthetic _id values using Lucene's stored fields API. The values are computed at runtime from the doc values of other
 * fields like _tsid, @timestamp and _ts_routing_hash.
 */
public class TSDBSyntheticIdStoredFieldsReader extends StoredFieldsReader {

    public static TSDBSyntheticIdStoredFieldsReader open(
        final Directory directory,
        final SegmentInfo si,
        final FieldInfos fn,
        final IOContext context
    ) throws IOException {
        Closeable closeable = null;
        boolean success = false;
        try {
            var fieldInfo = fieldInfo(fn);
            var docValuesProducer = si.getCodec().docValuesFormat().fieldsProducer(new SegmentReadState(directory, si, fn, context));
            closeable = docValuesProducer;
            var storedFieldsReader = new TSDBSyntheticIdStoredFieldsReader(directory, si, fn, context, docValuesProducer, fieldInfo);
            success = true;
            return storedFieldsReader;
        } finally {
            if (success == false) {
                IOUtils.close(closeable);
            }
        }
    }

    private final Directory directory;
    private final SegmentInfo segmentInfo;
    private final FieldInfos fieldInfos;
    private final IOContext context;
    private final DocValuesProducer docValuesProducer;
    private final FieldInfo fieldInfo;
    private final TSDBSyntheticIdDocValuesHolder docValuesHolder;

    private TSDBSyntheticIdStoredFieldsReader(
        Directory directory,
        SegmentInfo segmentInfo,
        FieldInfos fieldInfos,
        IOContext context,
        DocValuesProducer docValuesProducer,
        FieldInfo fieldInfo
    ) {
        this.directory = Objects.requireNonNull(directory);
        this.segmentInfo = Objects.requireNonNull(segmentInfo);
        this.fieldInfos = Objects.requireNonNull(fieldInfos);
        this.context = Objects.requireNonNull(context);
        this.docValuesProducer = Objects.requireNonNull(docValuesProducer);
        this.fieldInfo = Objects.requireNonNull(fieldInfo);
        this.docValuesHolder = new TSDBSyntheticIdDocValuesHolder(fieldInfos, docValuesProducer);
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        if (visitor.needsField(fieldInfo) == StoredFieldVisitor.Status.YES) {
            var uid = docValuesHolder.docSyntheticId(docID);
            visitor.binaryField(fieldInfo, uid.bytes);
        }
    }

    @Override
    public StoredFieldsReader getMergeInstance() {
        try {
            // Synthetic id stored fields are never merged, but some APIs use the merge instance for other purposes
            return open(directory, segmentInfo, fieldInfos, context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public StoredFieldsReader clone() {
        return new TSDBSyntheticIdStoredFieldsReader(
            directory,
            segmentInfo,
            fieldInfos,
            context,
            docValuesProducer.getMergeInstance(),
            fieldInfo(fieldInfos)
        );
    }

    @Override
    public void checkIntegrity() throws IOException {}

    @Override
    public void close() throws IOException {
        IOUtils.close(docValuesProducer);
    }

    private static FieldInfo fieldInfo(FieldInfos fn) {
        var fieldInfo = fn.fieldInfo(IdFieldMapper.NAME);
        if (fieldInfo == null || SyntheticIdField.hasSyntheticIdAttributes(fieldInfo.attributes()) == false) {
            throw new IllegalArgumentException("Field [" + IdFieldMapper.NAME + "] is not synthetic");
        }
        return fieldInfo;
    }
}
