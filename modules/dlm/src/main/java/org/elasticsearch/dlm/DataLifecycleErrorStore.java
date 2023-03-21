/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Provides a store for the errors DLM encounters.
 * It offers the functionality to record, retrieve, and clear errors for a specified target.
 * This class is thread safe.
 */
public class DataLifecycleErrorStore {

    private final ConcurrentMap<String, String> targetToError = new ConcurrentHashMap<>();

    /**
     * Records a string representation of the provided exception for the provided target.
     * If an error was already recorded for the provided target this will override that error.
     */
    public void recordError(String target, Exception e) {
        targetToError.put(target, org.elasticsearch.common.Strings.toString(((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, EMPTY_PARAMS, e);
            return builder;
        })));
    }

    /**
     * Clears the recorded error for the provided target (if any exists)
     */
    public void clearRecordedError(String target) {
        targetToError.remove(target);
    }

    /**
     * Clears all the errors recorded in the store.
     */
    public void clearStore() {
        targetToError.clear();
    }

    /**
     * Retrieves the recorderd error for the provided target.
     */
    @Nullable
    public String getError(String target) {
        return targetToError.get(target);
    }
}
