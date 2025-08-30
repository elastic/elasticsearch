/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.ElasticsearchException;

import java.util.HashSet;
import java.util.Set;

/**
 * Defines which fields need to be loaded from _ignored_source during a fetch.
 */
public record IgnoredFieldsSpec(Set<String> requiredIgnoredFields, IgnoredSourceFieldMapper.IgnoredSourceFormat format) {
    public static IgnoredFieldsSpec NONE = new IgnoredFieldsSpec(Set.of(), IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE);

    public boolean noRequirements() {
        return requiredIgnoredFields.isEmpty();
    }

    public IgnoredFieldsSpec merge(IgnoredFieldsSpec other) {
        if (this.format == IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE) {
            return other;
        }
        if (other.format == IgnoredSourceFieldMapper.IgnoredSourceFormat.NO_IGNORED_SOURCE) {
            return this;
        }
        if (other.requiredIgnoredFields.isEmpty()) {
            return this;
        }
        if (this.requiredIgnoredFields.isEmpty()) {
            return other;
        }

        if (this.format != other.format) {
            throw new ElasticsearchException(
                "failed to merge IgnoredFieldsSpec with differing formats " + this.format.name() + "," + other.format.name()
            );
        }

        Set<String> mergedFields = new HashSet<>(requiredIgnoredFields);
        mergedFields.addAll(other.requiredIgnoredFields);
        return new IgnoredFieldsSpec(mergedFields, format);
    }

    /**
     * Get the set of stored fields required to load the specified fields from _ignored_source.
     */
    public Set<String> requiredStoredFields() {
        return Set.of(IgnoredSourceFieldMapper.NAME);
    }
}
