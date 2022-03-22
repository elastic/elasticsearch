/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class LegacyAdaptingPerFieldPostingsFormat extends PostingsFormat {
    /** Name of this {@link PostingsFormat}. */
    public static final String PER_FIELD_NAME = "PerField40";

    /** {@link FieldInfo} attribute name used to store the format name for each field. */
    public static final String PER_FIELD_FORMAT_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".format";

    /** {@link FieldInfo} attribute name used to store the segment suffix name for each field. */
    public static final String PER_FIELD_SUFFIX_KEY = PerFieldPostingsFormat.class.getSimpleName() + ".suffix";

    /** Sole constructor. */
    protected LegacyAdaptingPerFieldPostingsFormat() {
        super(PER_FIELD_NAME);
    }

    static String getSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    protected PostingsFormat getPostingsFormat(String formatName) {
        throw new IllegalArgumentException(formatName);
    }

    private class FieldsWriter extends FieldsConsumer {
        final SegmentWriteState writeState;
        final List<Closeable> toClose = new ArrayList<Closeable>();

        FieldsWriter(SegmentWriteState writeState) {
            this.writeState = writeState;
        }

        @Override
        public void write(Fields fields, NormsProducer norms) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(toClose);
        }
    }

    private static class FieldsReader extends FieldsProducer {

        private final Map<String, FieldsProducer> fields = new TreeMap<>();
        private final Map<String, FieldsProducer> formats = new HashMap<>();
        private final String segment;

        // clone for merge
        FieldsReader(FieldsReader other) {
            Map<FieldsProducer, FieldsProducer> oldToNew = new IdentityHashMap<>();
            // First clone all formats
            for (Map.Entry<String, FieldsProducer> ent : other.formats.entrySet()) {
                FieldsProducer values = ent.getValue().getMergeInstance();
                formats.put(ent.getKey(), values);
                oldToNew.put(ent.getValue(), values);
            }

            // Then rebuild fields:
            for (Map.Entry<String, FieldsProducer> ent : other.fields.entrySet()) {
                FieldsProducer producer = oldToNew.get(ent.getValue());
                assert producer != null;
                fields.put(ent.getKey(), producer);
            }

            segment = other.segment;
        }

        FieldsReader(final SegmentReadState readState, LegacyAdaptingPerFieldPostingsFormat legacyAdaptingPerFieldPostingsFormat)
            throws IOException {

            // Read _X.per and init each format:
            boolean success = false;
            try {
                // Read field name -> format name
                for (FieldInfo fi : readState.fieldInfos) {
                    if (fi.getIndexOptions() != IndexOptions.NONE) {
                        final String fieldName = fi.name;
                        final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
                        if (formatName != null) {
                            // null formatName means the field is in fieldInfos, but has no postings!
                            final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
                            if (suffix == null) {
                                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
                            }
                            PostingsFormat format = legacyAdaptingPerFieldPostingsFormat.getPostingsFormat(formatName);
                            String segmentSuffix = getSuffix(formatName, suffix);
                            if (formats.containsKey(segmentSuffix) == false) {
                                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
                            }
                            fields.put(fieldName, formats.get(segmentSuffix));
                        }
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(formats.values());
                }
            }

            this.segment = readState.segmentInfo.name;
        }

        @Override
        public Iterator<String> iterator() {
            return Collections.unmodifiableSet(fields.keySet()).iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            FieldsProducer fieldsProducer = fields.get(field);
            return fieldsProducer == null ? null : fieldsProducer.terms(field);
        }

        @Override
        public int size() {
            return fields.size();
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formats.values());
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (FieldsProducer producer : formats.values()) {
                producer.checkIntegrity();
            }
        }

        @Override
        public FieldsProducer getMergeInstance() {
            return new FieldsReader(this);
        }

        @Override
        public String toString() {
            return "PerFieldPostings(segment=" + segment + " formats=" + formats.size() + ")";
        }
    }

    @Override
    public final FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FieldsWriter(state);
    }

    @Override
    public final FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state, this);
    }

    /**
     * Returns the postings format that should be used for writing new segments of <code>field</code>.
     *
     * <p>The field to format mapping is written to the index, so this method is only invoked when
     * writing, not when reading.
     */
    public abstract PostingsFormat getPostingsFormatForField(String field);
}
