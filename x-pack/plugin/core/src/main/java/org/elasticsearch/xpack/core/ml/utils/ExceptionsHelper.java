/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

public class ExceptionsHelper {

    private ExceptionsHelper() {}

    public static ResourceNotFoundException missingJobException(String jobId) {
        return new ResourceNotFoundException(Messages.getMessage(Messages.JOB_UNKNOWN_ID, jobId));
    }

    public static ResourceAlreadyExistsException jobAlreadyExists(String jobId) {
        return new ResourceAlreadyExistsException(Messages.getMessage(Messages.JOB_CONFIG_ID_ALREADY_TAKEN, jobId));
    }

    public static ResourceNotFoundException missingDatafeedException(String datafeedId) {
        return new ResourceNotFoundException(Messages.getMessage(Messages.DATAFEED_NOT_FOUND, datafeedId));
    }

    public static ResourceAlreadyExistsException datafeedAlreadyExists(String datafeedId) {
        return new ResourceAlreadyExistsException(Messages.getMessage(Messages.DATAFEED_ID_ALREADY_TAKEN, datafeedId));
    }

    public static ResourceNotFoundException missingDataFrameAnalytics(String id) {
        return new ResourceNotFoundException("No known data frame analytics with id [{}]", id);
    }

    public static ResourceAlreadyExistsException dataFrameAnalyticsAlreadyExists(String id) {
        return new ResourceAlreadyExistsException("A data frame analytics with id [{}] already exists", id);
    }

    public static ResourceNotFoundException missingModelDeployment(String deploymentId) {
        return new ResourceNotFoundException("No known model deployment with id [{}]", deploymentId);
    }

    public static ResourceNotFoundException missingTrainedModel(String modelId) {
        return new ResourceNotFoundException("No known trained model with model_id [{}]", modelId);
    }

    public static ResourceNotFoundException missingTrainedModel(String modelId, Exception cause) {
        return new ResourceNotFoundException("No known trained model with model_id [{}]", cause, modelId);
    }

    public static ElasticsearchException serverError(String msg) {
        return new ElasticsearchException(msg);
    }

    public static ElasticsearchException serverError(String msg, Throwable cause) {
        return new ElasticsearchException(msg, cause);
    }

    public static ElasticsearchException serverError(String msg, Object... args) {
        return new ElasticsearchException(msg, args);
    }

    public static ElasticsearchException serverError(String msg, Throwable cause, Object... args) {
        return new ElasticsearchException(msg, cause, args);
    }

    public static ElasticsearchStatusException conflictStatusException(String msg, Throwable cause, Object... args) {
        return new ElasticsearchStatusException(msg, RestStatus.CONFLICT, cause, args);
    }

    public static ElasticsearchStatusException conflictStatusException(String msg, Object... args) {
        return new ElasticsearchStatusException(msg, RestStatus.CONFLICT, args);
    }

    public static ElasticsearchStatusException badRequestException(String msg, Throwable cause, Object... args) {
        return new ElasticsearchStatusException(msg, RestStatus.BAD_REQUEST, cause, args);
    }

    public static ElasticsearchStatusException badRequestException(String msg, Object... args) {
        return new ElasticsearchStatusException(msg, RestStatus.BAD_REQUEST, args);
    }

    public static ElasticsearchStatusException taskOperationFailureToStatusException(TaskOperationFailure failure) {
        return new ElasticsearchStatusException(failure.getCause().getMessage(), failure.getStatus(), failure.getCause());
    }

    /**
     * Creates an error message that explains there are shard failures, displays info
     * for the first failure (shard/reason) and kindly asks to see more info in the logs
     */
    public static String shardFailuresToErrorMsg(String jobId, ShardSearchFailure[] shardFailures) {
        if (shardFailures == null || shardFailures.length == 0) {
            throw new IllegalStateException("Invalid call with null or empty shardFailures");
        }
        SearchShardTarget shardTarget = shardFailures[0].shard();
        return "["
            + jobId
            + "] Search request returned shard failures; first failure: shard ["
            + (shardTarget == null ? "_na" : shardTarget)
            + "], reason ["
            + shardFailures[0].reason()
            + "]; see logs for more info";
    }

    /**
     * A more REST-friendly Object.requireNonNull()
     */
    public static <T> T requireNonNull(T obj, String paramName) {
        if (obj == null) {
            throw new IllegalArgumentException("[" + paramName + "] must not be null.");
        }
        return obj;
    }

    public static <T> T requireNonNull(T obj, ParseField paramName) {
        return requireNonNull(obj, paramName.getPreferredName());
    }

    /**
     * @see org.elasticsearch.ExceptionsHelper#unwrapCause(Throwable)
     */
    public static Throwable unwrapCause(Throwable t) {
        return org.elasticsearch.ExceptionsHelper.unwrapCause(t);
    }

    /**
     * Unwrap the exception stack and return the most likely cause.
     * This method has special handling for {@link SearchPhaseExecutionException}
     * where it returns the cause of the first shard failure.
     *
     * @param t raw Throwable
     * @return unwrapped throwable if possible
     */
    public static Throwable findSearchExceptionRootCause(Throwable t) {
        // circuit breaking exceptions are at the bottom
        Throwable unwrappedThrowable = unwrapCause(t);

        if (unwrappedThrowable instanceof SearchPhaseExecutionException searchPhaseException) {
            for (ShardSearchFailure shardFailure : searchPhaseException.shardFailures()) {
                Throwable unwrappedShardFailure = unwrapCause(shardFailure.getCause());

                if (unwrappedShardFailure instanceof ElasticsearchException) {
                    return unwrappedShardFailure;
                }
            }
        }

        return unwrappedThrowable;
    }
}
