/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.perfield;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Fork of org.apache.lucene.codecs.perfield.PerFieldMergeState, because of {@link XPerFieldDocValuesFormat} */
final class XPerFieldMergeState {

    /**
     * Create a new MergeState from the given {@link MergeState} instance with restricted fields.
     *
     * @param fields The fields to keep in the new instance.
     * @return The new MergeState with restricted fields
     */
    static MergeState restrictFields(MergeState in, Collection<String> fields) {
        var fieldInfos = new FieldInfos[in.fieldInfos.length];
        for (int i = 0; i < in.fieldInfos.length; i++) {
            fieldInfos[i] = new FilterFieldInfos(in.fieldInfos[i], fields);
        }
        var fieldsProducers = new FieldsProducer[in.fieldsProducers.length];
        for (int i = 0; i < in.fieldsProducers.length; i++) {
            fieldsProducers[i] = in.fieldsProducers[i] == null ? null : new FilterFieldsProducer(in.fieldsProducers[i], fields);
        }
        var mergeFieldInfos = new FilterFieldInfos(in.mergeFieldInfos, fields);
        return new MergeState(
            in.docMaps,
            in.segmentInfo,
            mergeFieldInfos,
            in.storedFieldsReaders,
            in.termVectorsReaders,
            in.normsProducers,
            in.docValuesProducers,
            fieldInfos,
            in.liveDocs,
            fieldsProducers,
            in.pointsReaders,
            in.knnVectorsReaders,
            in.maxDocs,
            in.infoStream,
            in.intraMergeTaskExecutor,
            in.needsIndexSort
        );
    }

    private static class FilterFieldInfos extends FieldInfos {
        private final Set<String> filteredNames;
        private final List<FieldInfo> filtered;

        // Copy of the private fields from FieldInfos
        // Renamed so as to be less confusing about which fields we're referring to
        private final boolean filteredHasPostings;
        private final boolean filteredHasProx;
        private final boolean filteredHasPayloads;
        private final boolean filteredHasOffsets;
        private final boolean filteredHasFreq;
        private final boolean filteredHasNorms;
        private final boolean filteredHasDocValues;
        private final boolean filteredHasPointValues;

        FilterFieldInfos(FieldInfos src, Collection<String> filterFields) {
            // Copy all the input FieldInfo objects since the field numbering must be kept consistent
            super(toArray(src));

            boolean hasPostings = false;
            boolean hasProx = false;
            boolean hasPayloads = false;
            boolean hasOffsets = false;
            boolean hasFreq = false;
            boolean hasNorms = false;
            boolean hasDocValues = false;
            boolean hasPointValues = false;

            this.filteredNames = new HashSet<>(filterFields);
            this.filtered = new ArrayList<>(filterFields.size());
            for (FieldInfo fi : src) {
                if (this.filteredNames.contains(fi.name)) {
                    this.filtered.add(fi);
                    hasPostings |= fi.getIndexOptions() != IndexOptions.NONE;
                    hasProx |= fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
                    hasFreq |= fi.getIndexOptions() != IndexOptions.DOCS;
                    hasOffsets |= fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
                    hasNorms |= fi.hasNorms();
                    hasDocValues |= fi.getDocValuesType() != DocValuesType.NONE;
                    hasPayloads |= fi.hasPayloads();
                    hasPointValues |= (fi.getPointDimensionCount() != 0);
                }
            }

            this.filteredHasPostings = hasPostings;
            this.filteredHasProx = hasProx;
            this.filteredHasPayloads = hasPayloads;
            this.filteredHasOffsets = hasOffsets;
            this.filteredHasFreq = hasFreq;
            this.filteredHasNorms = hasNorms;
            this.filteredHasDocValues = hasDocValues;
            this.filteredHasPointValues = hasPointValues;
        }

        private static FieldInfo[] toArray(FieldInfos src) {
            FieldInfo[] res = new FieldInfo[src.size()];
            int i = 0;
            for (FieldInfo fi : src) {
                res[i++] = fi;
            }
            return res;
        }

        @Override
        public Iterator<FieldInfo> iterator() {
            return filtered.iterator();
        }

        @Override
        public boolean hasFreq() {
            return filteredHasFreq;
        }

        @Override
        public boolean hasPostings() {
            return filteredHasPostings;
        }

        @Override
        public boolean hasProx() {
            return filteredHasProx;
        }

        @Override
        public boolean hasPayloads() {
            return filteredHasPayloads;
        }

        @Override
        public boolean hasOffsets() {
            return filteredHasOffsets;
        }

        @Override
        public boolean hasNorms() {
            return filteredHasNorms;
        }

        @Override
        public boolean hasDocValues() {
            return filteredHasDocValues;
        }

        @Override
        public boolean hasPointValues() {
            return filteredHasPointValues;
        }

        @Override
        public int size() {
            return filtered.size();
        }

        @Override
        public FieldInfo fieldInfo(String fieldName) {
            if (filteredNames.contains(fieldName) == false) {
                // Throw IAE to be consistent with fieldInfo(int) which throws it as well on invalid numbers
                throw new IllegalArgumentException(
                    "The field named '"
                        + fieldName
                        + "' is not accessible in the current "
                        + "merge context, available ones are: "
                        + filteredNames
                );
            }
            return super.fieldInfo(fieldName);
        }

        @Override
        public FieldInfo fieldInfo(int fieldNumber) {
            FieldInfo res = super.fieldInfo(fieldNumber);
            if (filteredNames.contains(res.name) == false) {
                throw new IllegalArgumentException(
                    "The field named '"
                        + res.name
                        + "' numbered '"
                        + fieldNumber
                        + "' is not "
                        + "accessible in the current merge context, available ones are: "
                        + filteredNames
                );
            }
            return res;
        }
    }

    private static class FilterFieldsProducer extends FieldsProducer {
        private final FieldsProducer in;
        private final List<String> filtered;

        FilterFieldsProducer(FieldsProducer in, Collection<String> filterFields) {
            this.in = in;
            this.filtered = new ArrayList<>(filterFields);
        }

        @Override
        public Iterator<String> iterator() {
            return filtered.iterator();
        }

        @Override
        public Terms terms(String field) throws IOException {
            if (filtered.contains(field) == false) {
                throw new IllegalArgumentException(
                    "The field named '" + field + "' is not accessible in the current " + "merge context, available ones are: " + filtered
                );
            }
            return in.terms(field);
        }

        @Override
        public int size() {
            return filtered.size();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public void checkIntegrity() throws IOException {
            in.checkIntegrity();
        }
    }
}
