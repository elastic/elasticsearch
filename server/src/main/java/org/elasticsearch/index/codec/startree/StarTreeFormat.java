/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.mapper.startree.StarTreeConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Doc values format that builds star-trees for pre-aggregated analytics.
 * <p>
 * This format wraps another DocValuesFormat and adds star-tree building during segment creation/merge.
 * The star-tree is stored in auxiliary files alongside the regular doc values.
 */
public final class StarTreeFormat extends DocValuesFormat {

    private final DocValuesFormat delegate;
    private final StarTreeConfig.StarTreeFieldConfig config;

    /**
     * Create a star-tree format that wraps another format.
     *
     * @param delegate the underlying doc values format
     * @param config the star-tree configuration
     */
    public StarTreeFormat(DocValuesFormat delegate, StarTreeConfig.StarTreeFieldConfig config) {
        super(StarTreeConstants.CODEC_NAME);
        this.delegate = delegate;
        this.config = config;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        DocValuesConsumer delegateConsumer = delegate.fieldsConsumer(state);
        return new StarTreeDocValuesConsumer(delegateConsumer, state, config);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer delegateProducer = delegate.fieldsProducer(state);
        return new StarTreeDocValuesProducer(delegateProducer, state, config.getName());
    }

    /**
     * Consumer that writes doc values and builds star-tree.
     */
    private static final class StarTreeDocValuesConsumer extends DocValuesConsumer {

        private final DocValuesConsumer delegate;
        private final SegmentWriteState state;
        private final StarTreeConfig.StarTreeFieldConfig config;

        // Collected doc values for star-tree building
        private final Map<String, SortedDocValues> sortedGroupingFields = new HashMap<>();
        private final Map<String, SortedNumericDocValues> numericGroupingFields = new HashMap<>();
        private final Map<String, SortedNumericDocValues> valueFieldValues = new HashMap<>();

        StarTreeDocValuesConsumer(
            DocValuesConsumer delegate,
            SegmentWriteState state,
            StarTreeConfig.StarTreeFieldConfig config
        ) {
            this.delegate = delegate;
            this.state = state;
            this.config = config;
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addNumericField(field, valuesProducer);

            // Check if this is a grouping field (some numeric fields may be used as grouping)
            if (config.isGroupingField(field.name)) {
                SortedNumericDocValues sortedNumeric = valuesProducer.getSortedNumeric(field);
                if (sortedNumeric != null) {
                    numericGroupingFields.put(field.name, sortedNumeric);
                }
            }

            // Check if this is a value field
            if (config.isValue(field.name)) {
                SortedNumericDocValues sortedNumeric = valuesProducer.getSortedNumeric(field);
                if (sortedNumeric != null) {
                    valueFieldValues.put(field.name, sortedNumeric);
                }
            }
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addBinaryField(field, valuesProducer);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addSortedField(field, valuesProducer);

            // Check if this is a grouping field (keyword fields use SORTED doc values)
            if (config.isGroupingField(field.name)) {
                SortedDocValues sorted = valuesProducer.getSorted(field);
                if (sorted != null) {
                    sortedGroupingFields.put(field.name, sorted);
                }
            }
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addSortedNumericField(field, valuesProducer);

            // Check if this is a grouping field (date/numeric fields use SORTED_NUMERIC doc values)
            if (config.isGroupingField(field.name)) {
                SortedNumericDocValues sortedNumeric = valuesProducer.getSortedNumeric(field);
                if (sortedNumeric != null) {
                    numericGroupingFields.put(field.name, sortedNumeric);
                }
            }

            // Check if this is a value field
            if (config.isValue(field.name)) {
                SortedNumericDocValues sortedNumeric = valuesProducer.getSortedNumeric(field);
                if (sortedNumeric != null) {
                    valueFieldValues.put(field.name, sortedNumeric);
                }
            }
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            delegate.addSortedSetField(field, valuesProducer);
        }

        @Override
        public void close() throws IOException {
            try {
                // Build star-tree from collected values
                buildStarTree();
            } finally {
                delegate.close();
            }
        }

        private void buildStarTree() throws IOException {
            // Only build if we have the required grouping fields and values
            // A grouping field can be in either sortedGroupingFields or numericGroupingFields
            boolean hasAllGroupingFields = config.getGroupingFields().stream()
                .allMatch(gf -> sortedGroupingFields.containsKey(gf.getField())
                    || numericGroupingFields.containsKey(gf.getField()));
            boolean hasAllValues = config.getValues().stream()
                .allMatch(value -> valueFieldValues.containsKey(value.getField()));

            if (hasAllGroupingFields == false || hasAllValues == false) {
                // Missing required fields, skip star-tree building
                return;
            }

            try (StarTreeWriter writer = new StarTreeWriter(state, config)) {
                writer.build(sortedGroupingFields, numericGroupingFields, valueFieldValues);
            }
        }
    }

    /**
     * Producer that provides doc values and star-tree access.
     */
    private static final class StarTreeDocValuesProducer extends DocValuesProducer {

        private final DocValuesProducer delegate;
        private final StarTreeReader starTreeReader;

        StarTreeDocValuesProducer(DocValuesProducer delegate, SegmentReadState state, String starTreeName) throws IOException {
            this.delegate = delegate;
            StarTreeReader reader = null;
            try {
                reader = new StarTreeReader(state, starTreeName);
            } catch (IOException e) {
                // Star-tree files may not exist, that's OK
            }
            this.starTreeReader = reader;
        }

        /**
         * Get the star-tree reader for this segment.
         */
        public StarTreeReader getStarTreeReader() {
            return starTreeReader;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return delegate.getNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            return delegate.getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            return delegate.getSorted(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return delegate.getSortedNumeric(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            return delegate.getSortedSet(field);
        }

        @Override
        public org.apache.lucene.index.DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
            return delegate.getSkipper(field);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            try {
                if (starTreeReader != null) {
                    starTreeReader.close();
                }
            } finally {
                delegate.close();
            }
        }
    }
}
