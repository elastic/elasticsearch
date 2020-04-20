/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Map;

public interface ChangeCollector {

    boolean collectChanges(SearchResponse searchResponse);

    QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp);

    SearchSourceBuilder buildChangesQuery(SearchSourceBuilder sourceBuilder, Map<String, Object> position, int pageSize);

    void clear();

    Map<String, Object> getBucketPosition();

}
