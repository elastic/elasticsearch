/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface SemanticTextUtils {
    /**
     * This method converts the given {@code valueObj} into a list of strings.
     */
    static List<String> nodeStringValues(String field, Object valueObj) {
        if (valueObj instanceof Number || valueObj instanceof Boolean) {
            return List.of(valueObj.toString());
        } else if (valueObj instanceof String value) {
            return List.of(value);
        } else if (valueObj instanceof Collection<?> values) {
            List<String> valuesString = new ArrayList<>();
            for (var v : values) {
                if (v instanceof Number || v instanceof Boolean) {
                    valuesString.add(v.toString());
                } else if (v instanceof String value) {
                    valuesString.add(value);
                } else {
                    throw new ElasticsearchStatusException(
                        "Invalid format for field [{}], expected [String|Number|Boolean] got [{}]",
                        RestStatus.BAD_REQUEST,
                        field,
                        valueObj.getClass().getSimpleName()
                    );
                }
            }
            return valuesString;
        }
        throw new ElasticsearchStatusException(
            "Invalid format for field [{}], expected [String|Number|Boolean] got [{}]",
            RestStatus.BAD_REQUEST,
            field,
            valueObj.getClass().getSimpleName()
        );
    }
}
