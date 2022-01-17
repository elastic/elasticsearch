/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.function.BooleanSupplier;

public interface MlDataRemover {
    TimeValue DEFAULT_MAX_DURATION = TimeValue.timeValueHours(8L);

    void remove(float requestsPerSecond, ActionListener<Boolean> listener, BooleanSupplier isTimedOutSupplier);

    /**
     * Extract {@code fieldName} from {@code hit} and if it is a string
     * return the string else {@code null}.
     * @param hit The search hit
     * @param fieldName Field to find
     * @return value iff the docfield is present and it is a string. Otherwise {@code null}
     */
    default String stringFieldValueOrNull(SearchHit hit, String fieldName) {
        DocumentField docField = hit.field(fieldName);
        if (docField != null) {
            Object value = docField.getValue();
            if (value instanceof String str) {
                return str;
            }
        }
        return null;
    }
}
