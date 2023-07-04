/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Provides a store for the errors DLM encounters.
 * It offers the functionality to record, retrieve, and clear errors for a specified target.
 * This class is thread safe.
 */
public class DataLifecycleErrorStore {

    private final ConcurrentMap<String, String> indexNameToError = new ConcurrentHashMap<>();

    /**
     * Records a string representation of the provided exception for the provided index.
     * If an error was already recorded for the provided index this will override that error.
     */
    public void recordError(String indexName, Exception e) {
        indexNameToError.put(indexName, org.elasticsearch.common.Strings.toString(((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, EMPTY_PARAMS, e);
            return builder;
        })));
    }

    /**
     * Clears the recorded error for the provided index (if any exists)
     */
    public void clearRecordedError(String indexName) {
        indexNameToError.remove(indexName);
    }

    /**
     * Clears all the errors recorded in the store.
     */
    public void clearStore() {
        indexNameToError.clear();
    }

    /**
     * Retrieves the recorderd error for the provided index.
     */
    @Nullable
    public String getError(String indexName) {
        return indexNameToError.get(indexName);
    }

    /**
     * Return an immutable view (a snapshot) of the tracked indices at the moment this method is called.
     */
    public List<String> getAllIndices() {
        return List.copyOf(indexNameToError.keySet());
    }
}
