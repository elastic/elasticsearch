/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Provides a store for the errors the data stream lifecycle encounters.
 * It offers the functionality to record, retrieve, and clear errors for a specified target.
 * This class is thread safe.
 */
public class DataStreamLifecycleErrorStore {

    public static final int MAX_ERROR_MESSAGE_LENGTH = 1000;
    private final ConcurrentMap<String, ErrorEntry> indexNameToError = new ConcurrentHashMap<>();
    private final LongSupplier nowSupplier;

    public DataStreamLifecycleErrorStore(LongSupplier nowSupplier) {
        this.nowSupplier = nowSupplier;
    }

    /**
     * Records a string representation of the provided exception for the provided index.
     * If an error was already recorded for the provided index this will override that error.
     *
     * Returns the previously recorded error for the provided index, or null otherwise.
     */
    @Nullable
    public ErrorEntry recordError(String indexName, Exception e) {
        String exceptionToString = Strings.toString((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, EMPTY_PARAMS, e);
            return builder;
        });
        String newError = Strings.substring(exceptionToString, 0, MAX_ERROR_MESSAGE_LENGTH);
        ErrorEntry existingError = indexNameToError.get(indexName);
        long recordedTimestamp = nowSupplier.getAsLong();
        if (existingError == null) {
            indexNameToError.put(indexName, new ErrorEntry(recordedTimestamp, newError, recordedTimestamp, 0));
        } else {
            if (existingError.error().equals(newError)) {
                indexNameToError.put(indexName, ErrorEntry.incrementRetryCount(existingError, nowSupplier));
            } else {
                indexNameToError.put(indexName, new ErrorEntry(recordedTimestamp, newError, recordedTimestamp, 0));
            }
        }
        return existingError;
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
     * Retrieves the recorded error for the provided index.
     */
    @Nullable
    public ErrorEntry getError(String indexName) {
        return indexNameToError.get(indexName);
    }

    /**
     * Return an immutable view (a snapshot) of the tracked indices at the moment this method is called.
     */
    public Set<String> getAllIndices() {
        return Set.copyOf(indexNameToError.keySet());
    }
}
