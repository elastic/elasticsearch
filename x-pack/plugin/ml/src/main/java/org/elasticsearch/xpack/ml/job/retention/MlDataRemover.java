/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

public interface MlDataRemover {
    void remove(float requestsPerSecond, ActionListener<Boolean> listener, Supplier<Boolean> isTimedOutSupplier);

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
            if (value instanceof String) {
                return (String)value;
            }
        }
        return null;
    }
}
