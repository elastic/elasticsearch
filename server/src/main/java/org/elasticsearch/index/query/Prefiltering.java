/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.TransportVersion;

import java.util.List;

public interface Prefiltering<T extends QueryBuilder> {

    TransportVersion QUERY_PREFILTERING = TransportVersion.fromName("query_prefiltering");

    T setPrefilters(List<QueryBuilder> prefilters);

    List<QueryBuilder> getPrefilters();

    default void propagatePrefilters(List<QueryBuilder> targetQueries) {
        List<QueryBuilder> prefilters = getPrefilters();
        if (prefilters.isEmpty() == false) {
            for (QueryBuilder targetQuery : targetQueries) {
                if (targetQuery instanceof Prefiltering<?> prefilteredQuery) {
                    prefilteredQuery.setPrefilters(prefilters.stream().filter(q -> q != targetQuery).toList());
                }
            }
        }
    }
}
