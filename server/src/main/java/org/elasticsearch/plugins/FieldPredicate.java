/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.util.Set;
import java.util.function.Predicate;

/**
 * Filter for visible fields.
 */
public interface FieldPredicate extends Accountable, Predicate<String> {
    /**
     * The default field predicate applied, which doesn't filter anything. That means that by default get mappings, get index
     * get field mappings and field capabilities API will return every field that's present in the mappings.
     */
    FieldPredicate ACCEPT_ALL = new FieldPredicate() {
        @Override
        public boolean test(String field) {
            return true;
        }

        @Override
        public String modifyHash(String hash) {
            return hash;
        }

        @Override
        public long ramBytesUsed() {
            return 0; // Shared
        }

        @Override
        public String toString() {
            return "accept all";
        }
    };

    /**
     * Should this field be included?
     */
    @Override
    boolean test(String field);

    /**
     * The predicate to use for an index whose flattened fields have the full paths {@code flattenedFields}. A flattened field holds all its
     * keys under one name, so for the field itself and for a mapped sub-field below it the returned predicate may answer differently than
     * this one does for a plain field of that name. For every other path it answers the same. Callers derive it once per index and pass the
     * index's whole set, since an implementation may do costly work over the whole set on every call.
     */
    default FieldPredicate withFlattenedFields(Set<String> flattenedFields) {
        return this;
    }

    /**
     * Modify the {@link MappingMetadata#getSha256} to track any filtering this predicate
     * has performed on the list of fields.
     */
    String modifyHash(String hash);

    class And implements FieldPredicate {
        private static final long SHALLOW_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(And.class);

        private final FieldPredicate first;
        private final FieldPredicate second;

        public And(FieldPredicate first, FieldPredicate second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean test(String field) {
            return first.test(field) && second.test(field);
        }

        @Override
        public FieldPredicate withFlattenedFields(Set<String> flattenedFields) {
            return new And(first.withFlattenedFields(flattenedFields), second.withFlattenedFields(flattenedFields));
        }

        @Override
        public String modifyHash(String hash) {
            return second.modifyHash(first.modifyHash(hash));
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_RAM_BYTES_USED + first.ramBytesUsed() + second.ramBytesUsed();
        }

        @Override
        public String toString() {
            return first + " then " + second;
        }
    }
}
