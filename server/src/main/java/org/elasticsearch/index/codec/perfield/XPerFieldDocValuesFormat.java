/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perfield;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Fork of {@link PerFieldDocValuesFormat} to allow access FieldsReader's fields field, otherwise no changes.
 */
public abstract class XPerFieldDocValuesFormat extends DocValuesFormat {
    /** Name of this {@link DocValuesFormat}. */
    public static final String PER_FIELD_NAME = "ESPerFieldDV819";

    /** {@link FieldInfo} attribute name used to store the format name for each field. */
    // FORK note: usage of PerFieldDocValuesFormat is needed for bwc purposes.
    // (Otherwise, we load no fields from indices that use PerFieldDocValuesFormat)
    public static final String PER_FIELD_FORMAT_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".format";

    /** {@link FieldInfo} attribute name used to store the segment suffix name for each field. */
    // FORK note: usage of PerFieldDocValuesFormat is needed for bwc purposes.
    public static final String PER_FIELD_SUFFIX_KEY = PerFieldDocValuesFormat.class.getSimpleName() + ".suffix";

    /** Sole constructor. */
    protected XPerFieldDocValuesFormat() {
        super(PER_FIELD_NAME);
    }

    @Override
    public final DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new FieldsWriter(state);
    }

    record ConsumerAndSuffix(DocValuesConsumer consumer, int suffix) implements Closeable {
        @Override
        public void close() throws IOException {
            consumer.close();
        }
    }

    @SuppressForbidden(reason = "forked from Lucene")
    private class FieldsWriter extends DocValuesConsumer {

        private final Map<DocValuesFormat, ConsumerAndSuffix> formats = new HashMap<>();
        private final Map<String, Integer> suffixes = new HashMap<>();

        private final SegmentWriteState segmentWriteState;

        FieldsWriter(SegmentWriteState state) {
            segmentWriteState = state;
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addNumericField(field, valuesProducer);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addBinaryField(field, valuesProducer);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedField(field, valuesProducer);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedNumericField(field, valuesProducer);
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            getInstance(field).addSortedSetField(field, valuesProducer);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            Map<DocValuesConsumer, Collection<String>> consumersToField = new IdentityHashMap<>();

            // Group each consumer by the fields it handles
            for (FieldInfo fi : mergeState.mergeFieldInfos) {
                if (fi.getDocValuesType() == DocValuesType.NONE) {
                    continue;
                }
                // merge should ignore current format for the fields being merged
                DocValuesConsumer consumer = getInstance(fi, true);
                Collection<String> fieldsForConsumer = consumersToField.get(consumer);
                if (fieldsForConsumer == null) {
                    fieldsForConsumer = new ArrayList<>();
                    consumersToField.put(consumer, fieldsForConsumer);
                }
                fieldsForConsumer.add(fi.name);
            }

            // Delegate the merge to the appropriate consumer
            for (Map.Entry<DocValuesConsumer, Collection<String>> e : consumersToField.entrySet()) {
                e.getKey().merge(XPerFieldMergeState.restrictFields(mergeState, e.getValue()));
            }
        }

        private DocValuesConsumer getInstance(FieldInfo field) throws IOException {
            return getInstance(field, false);
        }

        /**
         * DocValuesConsumer for the given field.
         *
         * @param field - FieldInfo object.
         * @param ignoreCurrentFormat - ignore the existing format attributes.
         * @return DocValuesConsumer for the field.
         * @throws IOException if there is a low-level IO error
         */
        private DocValuesConsumer getInstance(FieldInfo field, boolean ignoreCurrentFormat) throws IOException {
            DocValuesFormat format = null;
            if (field.getDocValuesGen() != -1) {
                String formatName = null;
                if (ignoreCurrentFormat == false) {
                    formatName = field.getAttribute(PER_FIELD_FORMAT_KEY);
                }
                // this means the field never existed in that segment, yet is applied updates
                if (formatName != null) {
                    format = DocValuesFormat.forName(formatName);
                }
            }
            if (format == null) {
                format = getDocValuesFormatForField(field.name);
            }
            if (format == null) {
                throw new IllegalStateException("invalid null DocValuesFormat for field=\"" + field.name + "\"");
            }
            final String formatName = format.getName();

            field.putAttribute(PER_FIELD_FORMAT_KEY, formatName);
            Integer suffix = null;

            ConsumerAndSuffix consumer = formats.get(format);
            if (consumer == null) {
                // First time we are seeing this format; create a new instance

                if (field.getDocValuesGen() != -1) {
                    String suffixAtt = null;
                    if (ignoreCurrentFormat == false) {
                        suffixAtt = field.getAttribute(PER_FIELD_SUFFIX_KEY);
                    }
                    // even when dvGen is != -1, it can still be a new field, that never
                    // existed in the segment, and therefore doesn't have the recorded
                    // attributes yet.
                    if (suffixAtt != null) {
                        suffix = Integer.valueOf(suffixAtt);
                    }
                }

                if (suffix == null) {
                    // bump the suffix
                    suffix = suffixes.get(formatName);
                    if (suffix == null) {
                        suffix = 0;
                    } else {
                        suffix = suffix + 1;
                    }
                }
                suffixes.put(formatName, suffix);

                final String segmentSuffix = getFullSegmentSuffix(
                    segmentWriteState.segmentSuffix,
                    getSuffix(formatName, Integer.toString(suffix))
                );
                consumer = new ConsumerAndSuffix(format.fieldsConsumer(new SegmentWriteState(segmentWriteState, segmentSuffix)), suffix);
                formats.put(format, consumer);
            } else {
                // we've already seen this format, so just grab its suffix
                assert suffixes.containsKey(formatName);
                suffix = consumer.suffix;
            }

            field.putAttribute(PER_FIELD_SUFFIX_KEY, Integer.toString(suffix));
            // TODO: we should only provide the "slice" of FIS
            // that this DVF actually sees ...
            return consumer.consumer;
        }

        @Override
        public void close() throws IOException {
            // Close all subs
            IOUtils.close(formats.values());
        }
    }

    static String getSuffix(String formatName, String suffix) {
        return formatName + "_" + suffix;
    }

    static String getFullSegmentSuffix(String outerSegmentSuffix, String segmentSuffix) {
        if (outerSegmentSuffix.length() == 0) {
            return segmentSuffix;
        } else {
            return outerSegmentSuffix + "_" + segmentSuffix;
        }
    }

    @SuppressForbidden(reason = "forked from Lucene")
    public static class FieldsReader extends DocValuesProducer {

        private final IntObjectHashMap<DocValuesProducer> fields = new IntObjectHashMap<>();
        private final Map<String, DocValuesProducer> formats = new HashMap<>();

        // clone for merge
        FieldsReader(FieldsReader other) {
            Map<DocValuesProducer, DocValuesProducer> oldToNew = new IdentityHashMap<>();
            // First clone all formats
            for (Map.Entry<String, DocValuesProducer> ent : other.formats.entrySet()) {
                DocValuesProducer values = ent.getValue().getMergeInstance();
                formats.put(ent.getKey(), values);
                oldToNew.put(ent.getValue(), values);
            }

            // Then rebuild fields:
            for (IntObjectHashMap.IntObjectCursor<DocValuesProducer> ent : other.fields) {
                DocValuesProducer producer = oldToNew.get(ent.value);
                assert producer != null;
                fields.put(ent.key, producer);
            }
        }

        FieldsReader(final SegmentReadState readState) throws IOException {

            // Init each unique format:
            boolean success = false;
            try {
                // Read field name -> format name
                for (FieldInfo fi : readState.fieldInfos) {
                    if (fi.getDocValuesType() != DocValuesType.NONE) {
                        final String fieldName = fi.name;
                        final String formatName = fi.getAttribute(PER_FIELD_FORMAT_KEY);
                        if (formatName != null) {
                            // null formatName means the field is in fieldInfos, but has no docvalues!
                            final String suffix = fi.getAttribute(PER_FIELD_SUFFIX_KEY);
                            if (suffix == null) {
                                throw new IllegalStateException("missing attribute: " + PER_FIELD_SUFFIX_KEY + " for field: " + fieldName);
                            }
                            DocValuesFormat format = DocValuesFormat.forName(formatName);
                            String segmentSuffix = getFullSegmentSuffix(readState.segmentSuffix, getSuffix(formatName, suffix));
                            if (formats.containsKey(segmentSuffix) == false) {
                                formats.put(segmentSuffix, format.fieldsProducer(new SegmentReadState(readState, segmentSuffix)));
                            }
                            fields.put(fi.number, formats.get(segmentSuffix));
                        }
                    }
                }
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(formats.values());
                }
            }
        }

        // FORK note: the reason why PerFieldDocValuesFormat is forked:
        public DocValuesProducer getDocValuesProducer(FieldInfo field) {
            return fields.get(field.number);
        }

        public Map<String, DocValuesProducer> getFormats() {
            return formats;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.number);
            return producer == null ? null : producer.getNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.number);
            return producer == null ? null : producer.getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.number);
            return producer == null ? null : producer.getSorted(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.number);
            return producer == null ? null : producer.getSortedNumeric(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            DocValuesProducer producer = fields.get(field.number);
            return producer == null ? null : producer.getSortedSet(field);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(formats.values());
        }

        @Override
        public void checkIntegrity() throws IOException {
            for (DocValuesProducer format : formats.values()) {
                format.checkIntegrity();
            }
        }

        @Override
        public DocValuesProducer getMergeInstance() {
            return new FieldsReader(this);
        }

        @Override
        public String toString() {
            return "PerFieldDocValues(formats=" + formats.size() + ")";
        }
    }

    @Override
    public final DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new FieldsReader(state);
    }

    /**
     * Returns the doc values format that should be used for writing new segments of <code>field
     * </code>.
     *
     * <p>The field to format mapping is written to the index, so this method is only invoked when
     * writing, not when reading.
     */
    public abstract DocValuesFormat getDocValuesFormatForField(String field);
}
