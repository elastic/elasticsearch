/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.startree;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.index.mapper.startree.StarTreeConfig;
import org.elasticsearch.index.mapper.startree.StarTreeGroupingField;
import org.elasticsearch.index.mapper.startree.StarTreeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder for creating star-tree indices from segment data.
 * <p>
 * This class provides a utility to build star-trees from existing segment doc values.
 * It can be called during segment flush or merge to create star-tree auxiliary files.
 */
public final class StarTreeBuilder {

    private StarTreeBuilder() {}

    /**
     * Build a star-tree for the given segment using the provided configuration.
     *
     * @param reader the leaf reader containing the segment data
     * @param directory the directory to write star-tree files to
     * @param segmentInfo the segment info
     * @param fieldInfos the field infos for the segment
     * @param config the star-tree configuration
     * @throws IOException if an I/O error occurs
     */
    public static void build(
        LeafReader reader,
        Directory directory,
        SegmentInfo segmentInfo,
        FieldInfos fieldInfos,
        StarTreeConfig.StarTreeFieldConfig config
    ) throws IOException {
        // Create write state for star-tree writer
        SegmentWriteState writeState = new SegmentWriteState(
            InfoStream.getDefault(),
            directory,
            segmentInfo,
            fieldInfos,
            null,  // segUpdates
            IOContext.DEFAULT
        );

        // Collect grouping field and value doc values
        Map<String, SortedDocValues> sortedGroupingFields = new HashMap<>();
        Map<String, SortedNumericDocValues> numericGroupingFields = new HashMap<>();
        Map<String, SortedNumericDocValues> valueDocValues = new HashMap<>();

        // Collect grouping field values (keyword fields use SORTED, date/numeric use SORTED_NUMERIC)
        for (StarTreeGroupingField gf : config.getGroupingFields()) {
            String fieldName = gf.getField();
            FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
            if (fieldInfo != null) {
                DocValuesType dvType = fieldInfo.getDocValuesType();
                if (dvType == DocValuesType.SORTED) {
                    SortedDocValues dv = reader.getSortedDocValues(fieldName);
                    if (dv != null) {
                        sortedGroupingFields.put(fieldName, dv);
                    }
                } else if (dvType == DocValuesType.SORTED_NUMERIC) {
                    SortedNumericDocValues dv = reader.getSortedNumericDocValues(fieldName);
                    if (dv != null) {
                        numericGroupingFields.put(fieldName, dv);
                    }
                } else if (dvType == DocValuesType.NUMERIC) {
                    // Wrap NumericDocValues as SortedNumericDocValues
                    var numericDv = reader.getNumericDocValues(fieldName);
                    if (numericDv != null) {
                        numericGroupingFields.put(fieldName, new SingleValueSortedNumericDocValues(numericDv));
                    }
                }
            }
        }

        // Collect value field doc values
        for (StarTreeValue value : config.getValues()) {
            String fieldName = value.getField();
            FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
            if (fieldInfo != null) {
                DocValuesType dvType = fieldInfo.getDocValuesType();
                if (dvType == DocValuesType.SORTED_NUMERIC) {
                    SortedNumericDocValues dv = reader.getSortedNumericDocValues(fieldName);
                    if (dv != null) {
                        valueDocValues.put(fieldName, dv);
                    }
                } else if (dvType == DocValuesType.NUMERIC) {
                    // Wrap NumericDocValues as SortedNumericDocValues
                    var numericDv = reader.getNumericDocValues(fieldName);
                    if (numericDv != null) {
                        valueDocValues.put(fieldName, new SingleValueSortedNumericDocValues(numericDv));
                    }
                }
            }
        }

        // Build the star-tree
        try (StarTreeWriter writer = new StarTreeWriter(writeState, config)) {
            writer.build(sortedGroupingFields, numericGroupingFields, valueDocValues);
        }
    }

    /**
     * Build a star-tree from a committed segment.
     *
     * @param segmentCommitInfo the segment commit info
     * @param directory the directory containing the segment
     * @param config the star-tree configuration
     * @throws IOException if an I/O error occurs
     */
    public static void buildFromCommittedSegment(
        SegmentCommitInfo segmentCommitInfo,
        Directory directory,
        StarTreeConfig.StarTreeFieldConfig config
    ) throws IOException {
        // This would need to open a SegmentReader for the committed segment
        // and call build() with it. For now, this is a placeholder.
        throw new UnsupportedOperationException("Building from committed segment not yet implemented");
    }

    /**
     * Check if star-tree files exist for a segment.
     *
     * @param directory the directory
     * @param segmentName the segment name
     * @param starTreeName the star-tree name
     * @return true if star-tree files exist
     */
    public static boolean starTreeExists(Directory directory, String segmentName, String starTreeName) throws IOException {
        String metaFileName = segmentName + "_" + starTreeName + "." + StarTreeConstants.META_EXTENSION;
        try {
            directory.openInput(metaFileName, IOContext.READONCE).close();
            return true;
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            return false;
        }
    }

    /**
     * Get a directory that can read star-tree files.
     * Note: Currently only supports standalone files. Compound file format is not supported.
     * To use star-tree, set index.compound_format=false on the index.
     *
     * @param directory the base directory
     * @param segmentName the segment name
     * @param starTreeName the star-tree name
     * @return the directory if star-tree files exist, or null otherwise
     */
    public static Directory getStarTreeDirectory(Directory directory, String segmentName, String starTreeName) throws IOException {
        if (starTreeExists(directory, segmentName, starTreeName)) {
            return directory;
        }
        return null;
    }

    /**
     * Wrapper to adapt NumericDocValues to SortedNumericDocValues interface.
     */
    private static class SingleValueSortedNumericDocValues extends SortedNumericDocValues {
        private final org.apache.lucene.index.NumericDocValues delegate;
        private boolean hasValue;

        SingleValueSortedNumericDocValues(org.apache.lucene.index.NumericDocValues delegate) {
            this.delegate = delegate;
        }

        @Override
        public long nextValue() throws IOException {
            return delegate.longValue();
        }

        @Override
        public int docValueCount() {
            return hasValue ? 1 : 0;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            hasValue = delegate.advanceExact(target);
            return hasValue;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return delegate.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return delegate.advance(target);
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }
}
