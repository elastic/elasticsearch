/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch;

import java.util.HashSet;
import java.util.Set;

public record StoredFieldsSpec(boolean requiresSource, Set<String> requiredStoredFields) {

    public static StoredFieldsSpec NO_REQUIREMENTS = new StoredFieldsSpec(false, Set.of());

    public static StoredFieldsSpec NEEDS_SOURCE = new StoredFieldsSpec(true, Set.of());

    public StoredFieldsSpec merge(StoredFieldsSpec other) {
        Set<String> mergedFields = new HashSet<>(this.requiredStoredFields);
        mergedFields.addAll(other.requiredStoredFields);
        return new StoredFieldsSpec(this.requiresSource || other.requiresSource, mergedFields);
    }
}
