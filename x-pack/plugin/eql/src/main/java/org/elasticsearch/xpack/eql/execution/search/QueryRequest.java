/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.search.builder.SearchSourceBuilder;

public interface QueryRequest {

    SearchSourceBuilder searchSource();

    default void nextAfter(Ordinal ordinal) {
        searchSource().searchAfter(ordinal.toArray());
    }
}
