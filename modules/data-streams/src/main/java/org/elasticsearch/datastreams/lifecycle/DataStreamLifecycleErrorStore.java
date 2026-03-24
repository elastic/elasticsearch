/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.datastreams.lifecycle.ErrorEntry;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.node.DslErrorInfo;
import org.elasticsearch.health.node.ProjectIndexName;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.LogManager.getLogger;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Provides a store for the errors the data stream lifecycle encounters.
 * It offers the functionality to record, retrieve, and clear errors for a specified target.
 * This class is thread safe.
 */
public class DataStreamLifecycleErrorStore {

    private static final Logger logger = getLogger(DataStreamLifecycleErrorStore.class);

    public static final int MAX_ERROR_MESSAGE_LENGTH = 1000;
    private final ConcurrentMap<ProjectId, ConcurrentMap<String, ErrorEntry>> projectMap = new ConcurrentHashMap<>();
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
    public ErrorEntry recordError(ProjectId projectId, String indexName, Exception e) {
        String exceptionToString = Strings.toString((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, EMPTY_PARAMS, e);
            return builder;
        });
        String newError = Strings.substring(exceptionToString, 0, MAX_ERROR_MESSAGE_LENGTH);
        final var indexNameToError = projectMap.computeIfAbsent(projectId, k -> new ConcurrentHashMap<>());
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
    public void clearRecordedError(ProjectId projectId, String indexName) {
        final var indexNameToError = projectMap.get(projectId);
        if (indexNameToError == null) {
            return;
        }
        indexNameToError.remove(indexName);
    }

    /**
     * Clears all the errors recorded in the store.
     */
    public void clearStore() {
        projectMap.clear();
    }

    /**
     * Retrieves the recorded error for the provided index.
     */
    @Nullable
    public ErrorEntry getError(ProjectId projectId, String indexName) {
        final var indexNameToError = projectMap.get(projectId);
        if (indexNameToError == null) {
            return null;
        }
        return indexNameToError.get(indexName);
    }

    /**
     * Return an immutable view (a snapshot) of the tracked indices at the moment this method is called.
     */
    public Set<String> getAllIndices(ProjectId projectId) {
        final var indexNameToError = projectMap.get(projectId);
        if (indexNameToError == null) {
            return Set.of();
        }
        return Set.copyOf(indexNameToError.keySet());
    }

    /**
     * Retrieve the error entries in the error store that satisfy the provided predicate.
     * This will return the error entries information (a subset of all the fields an {@link ErrorEntry} holds) sorted by the number of
     * retries DSL attempted (descending order) and the number of entries will be limited according to the provided limit parameter.
     * Returns empty list if no entries are present in the error store or none satisfy the predicate.
     */
    public List<DslErrorInfo> getErrorsInfo(Predicate<ErrorEntry> errorEntryPredicate, int limit) {
        return projectMap.entrySet()
            .stream()
            .flatMap(
                projectToIndexError -> projectToIndexError.getValue()
                    .entrySet()
                    .stream()
                    .map(
                        indexToError -> new Tuple<>(
                            new ProjectIndexName(projectToIndexError.getKey(), indexToError.getKey()),
                            indexToError.getValue()
                        )
                    )
            )
            .filter(projectIndexAndError -> errorEntryPredicate.test(projectIndexAndError.v2()))
            .sorted(Comparator.comparing(Tuple::v2))
            .limit(limit)
            .map(
                projectIndexAndError -> new DslErrorInfo(
                    projectIndexAndError.v1().indexName(),
                    projectIndexAndError.v2().firstOccurrenceTimestamp(),
                    projectIndexAndError.v2().retryCount(),
                    projectIndexAndError.v1().projectId()
                )
            )
            .collect(Collectors.toList());
    }

    /**
     * Get the total number of error entries in the store
     */
    public int getTotalErrorEntries() {
        return projectMap.values().stream().mapToInt(Map::size).sum();
    }

    /**
     * Records the provided error for the index in the error store and logs the error message at `ERROR` level if the error for the index
     * is different to what's already in the error store or if the same error was in the error store for a number of retries divisible by
     * the provided signallingErrorRetryThreshold (i.e. we log to level `error` every signallingErrorRetryThreshold retries, if the error
     * stays the same)
     * This allows us to not spam the logs, but signal to the logs if DSL is not making progress.
     */
    public void recordAndLogError(
        ProjectId projectId,
        String targetIndex,
        Exception e,
        String logMessage,
        int signallingErrorRetryThreshold
    ) {
        ErrorEntry previousError = recordError(projectId, targetIndex, e);
        ErrorEntry currentError = getError(projectId, targetIndex);

        if (previousError == null || (currentError != null && previousError.error().equals(currentError.error()) == false)) {
            logger.warn(logMessage, e);
        } else {
            if (currentError != null) {
                Message message = logger.getMessageFactory()
                    .newMessage(
                        "{}\nFailing since [{}], operation retried [{}] times",
                        logMessage,
                        currentError.firstOccurrenceTimestamp(),
                        currentError.retryCount()
                    );
                if (currentError.retryCount() % signallingErrorRetryThreshold == 0) {
                    logger.warn(message, e);
                } else {
                    logger.trace(message, e);
                }
            } else {
                logger.trace(
                    logger.getMessageFactory()
                        .newMessage(
                            "Index [{}] encountered error [{}] but there's no record in the error store anymore",
                            targetIndex,
                            logMessage
                        ),
                    e
                );
            }
        }
    }
}
