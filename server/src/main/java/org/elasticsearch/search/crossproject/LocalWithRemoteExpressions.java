/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A container for a local expression and a list of remote expressions.
 */
public record LocalWithRemoteExpressions(@Nullable String localExpression, List<String> remoteExpressions) {
    public LocalWithRemoteExpressions(String localExpression) {
        this(localExpression, List.of());
    }

    List<String> all() {
        if (localExpression == null) {
            return remoteExpressions;
        }
        List<String> all = new ArrayList<>();
        all.add(localExpression);
        all.addAll(remoteExpressions);
        return List.copyOf(all);
    }
}
