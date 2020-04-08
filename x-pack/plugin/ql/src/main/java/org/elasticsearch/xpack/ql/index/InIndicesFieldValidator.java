/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.ql.index.IndexResolver.UNMAPPED;
import static org.elasticsearch.xpack.ql.type.DataTypes.CONSTANT_KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;

/*
 * Checks a field in the field_caps output for incompatibilities across multiple indices.
 */
class InIndicesFieldValidator implements FieldValidator {
    
    /*
     * Returns an empty map or a single value map where the key is the field name and the value 
     * is an invalid field object with error messages.
     */
    @Override
    public Map<String, InvalidMappedField> validateField(String fieldName, Map<String, FieldCapabilities> types,
            ImmutableOpenMap<String, List<AliasMetadata>> aliases) {
        StringBuilder errorMessage = new StringBuilder();

        boolean hasUnmapped = types.containsKey(UNMAPPED);
        // a keyword field and a constant_keyword field with the same name in two different indices are considered "compatible"
        // since a common use case of constant_keyword field involves two indices with a field having the same name: one being
        // a keyword, the other being a constant_keyword
        boolean hasCompatibleKeywords = types.containsKey(KEYWORD.esType()) && types.containsKey(CONSTANT_KEYWORD.esType());
        int allowedTypesCount = (hasUnmapped ? 2 : 1) + (hasCompatibleKeywords ? 1 : 0);

        if (types.size() > allowedTypesCount) {
            // build the error message
            // and create a MultiTypeField

            for (Entry<String, FieldCapabilities> type : types.entrySet()) {
                // skip unmapped
                if (UNMAPPED.equals(type.getKey())) {
                    continue;
                }

                if (errorMessage.length() > 0) {
                    errorMessage.append(", ");
                }
                errorMessage.append("[");
                errorMessage.append(type.getKey());
                errorMessage.append("] in ");
                errorMessage.append(Arrays.toString(type.getValue().indices()));
            }

            errorMessage.insert(0, "mapped as [" + (types.size() - (hasUnmapped ? 1 : 0)) + "] incompatible types: ");

            return Collections.singletonMap(fieldName, new InvalidMappedField(fieldName, errorMessage.toString()));
        }
        // type is okay, check aggregation
        else {
            FieldCapabilities fieldCap = types.values().iterator().next();

            // validate search/agg-able
            if (fieldCap.isAggregatable() && fieldCap.nonAggregatableIndices() != null) {
                errorMessage.append("mapped as aggregatable except in ");
                errorMessage.append(Arrays.toString(fieldCap.nonAggregatableIndices()));
            }
            if (fieldCap.isSearchable() && fieldCap.nonSearchableIndices() != null) {
                if (errorMessage.length() > 0) {
                    errorMessage.append(",");
                }
                errorMessage.append("mapped as searchable except in ");
                errorMessage.append(Arrays.toString(fieldCap.nonSearchableIndices()));
            }

            if (errorMessage.length() > 0) {
                return Collections.singletonMap(fieldName, new InvalidMappedField(fieldName, errorMessage.toString()));
            }
        }

        // if there are both a keyword and a constant_keyword type for this field, only keep the keyword as a common compatible type
        if (hasCompatibleKeywords) {
            types.remove(CONSTANT_KEYWORD.esType());
        }

        // everything checks
        return emptyMap();
    }

}
