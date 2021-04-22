/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestRequest;

import java.util.Set;
import java.util.function.Function;

public class TypeConsumer implements Function<String, Boolean> {
    private final RestRequest request;
    private final Set<String> fieldNames;
    private boolean foundTypeInBody = false;

    public TypeConsumer(RestRequest request, String... fieldNames) {
        this.request = request;
        this.fieldNames = Set.of(fieldNames);
    }

    @Override
    public Boolean apply(String fieldName) {
        if (fieldNames.contains(fieldName)) {
            foundTypeInBody = true;
            return true;
        }
        return false;
    }

    public boolean hasTypes() {
        // TODO can params be types too? or _types?
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        return types.length > 0 || foundTypeInBody;
    }
}
