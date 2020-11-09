/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SearchPhaseExecutionException extends ElasticsearchException {
    private final String phaseName;
    private final ShardSearchFailure[] shardFailures;

    public SearchPhaseExecutionException(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        this(phaseName, msg, null, shardFailures);
    }

    public SearchPhaseExecutionException(String phaseName, String msg, Throwable cause, ShardSearchFailure[] shardFailures) {
        super(msg, deduplicateCause(cause, shardFailures));
        this.phaseName = phaseName;
        this.shardFailures = shardFailures;
    }

    public SearchPhaseExecutionException(StreamInput in) throws IOException {
        super(in);
        phaseName = in.readOptionalString();
        shardFailures = in.readArray(ShardSearchFailure::readShardSearchFailure, ShardSearchFailure[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(phaseName);
        out.writeArray(shardFailures);
    }

    private static Throwable deduplicateCause(Throwable cause, ShardSearchFailure[] shardFailures) {
        if (shardFailures == null) {
            throw new IllegalArgumentException("shardSearchFailures must not be null");
        }
        // if the cause of this exception is also the cause of one of the shard failures we don't add it
        // to prevent duplication in stack traces rendered to the REST layer
        if (cause != null) {
            for (ShardSearchFailure failure : shardFailures) {
                if (failure.getCause() == cause) {
                    return null;
                }
            }
        }
        return cause;
    }

    @Override
    public RestStatus status() {
        if (shardFailures.length == 0) {
            // if no successful shards, the failure can be due to EsRejectedExecutionException during fetch phase
            // on coordinator node. so get the status from cause instead of returning SERVICE_UNAVAILABLE blindly
            return getCause() == null ? RestStatus.SERVICE_UNAVAILABLE : ExceptionsHelper.status(getCause());
        }
        RestStatus status = shardFailures[0].status();
        if (shardFailures.length > 1) {
            for (int i = 1; i < shardFailures.length; i++) {
                if (shardFailures[i].status().getStatus() >= 500) {
                    status = shardFailures[i].status();
                }
            }
        }
        return status;
    }

    public ShardSearchFailure[] shardFailures() {
        return shardFailures;
    }

    @Override
    public Throwable getCause() {
        Throwable cause = super.getCause();
        if (cause == null) {
            // fall back to guessed root cause
            for (ElasticsearchException rootCause : guessRootCauses()) {
                return rootCause;
            }
        }
        return cause;
    }

    private static String buildMessage(String phaseName, String msg, ShardSearchFailure[] shardFailures) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed to execute phase [").append(phaseName).append("], ").append(msg);
        if (CollectionUtils.isEmpty(shardFailures) == false) {
            sb.append("; shardFailures ");
            for (ShardSearchFailure shardFailure : shardFailures) {
                if (shardFailure.shard() != null) {
                    sb.append("{").append(shardFailure.shard()).append(": ").append(shardFailure.reason()).append("}");
                } else {
                    sb.append("{").append(shardFailure.reason()).append("}");
                }
            }
        }
        return sb.toString();
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("phase", phaseName);
        builder.field("grouped", true); // notify that it's grouped
        builder.field("failed_shards");
        builder.startArray();
        ShardOperationFailedException[] failures = ExceptionsHelper.groupBy(shardFailures);
        for (ShardOperationFailedException failure : failures) {
            failure.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Throwable ex = ExceptionsHelper.unwrapCause(this);
        if (ex != this) {
            generateThrowableXContent(builder, params, this);
        } else {
            // We don't have a cause when all shards failed, but we do have shards failures so we can "guess" a cause
            // (see {@link #getCause()}). Here, we use super.getCause() because we don't want the guessed exception to
            // be rendered twice (one in the "cause" field, one in "failed_shards")
            innerToXContent(builder, params, this, getExceptionName(), getMessage(), getHeaders(), getMetadata(), super.getCause());
        }
        return builder;
    }

    @Override
    public ElasticsearchException[] guessRootCauses() {
        ShardOperationFailedException[] failures = ExceptionsHelper.groupBy(shardFailures);
        List<ElasticsearchException> rootCauses = new ArrayList<>(failures.length);
        for (ShardOperationFailedException failure : failures) {
            ElasticsearchException[] guessRootCauses = ElasticsearchException.guessRootCauses(failure.getCause());
            rootCauses.addAll(Arrays.asList(guessRootCauses));
        }
        return rootCauses.toArray(new ElasticsearchException[0]);
    }

    @Override
    public String toString() {
        return buildMessage(phaseName, getMessage(), shardFailures);
    }

    public String getPhaseName() {
        return phaseName;
    }
}
