/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest.MIN_KEEP_ALIVE;

public class GetAsyncSearchAction extends ActionType<AsyncSearchResponse> {
    public static final GetAsyncSearchAction INSTANCE = new GetAsyncSearchAction();
    public static final String NAME = "indices:data/read/async_search/get";

    private GetAsyncSearchAction() {
        super(NAME, AsyncSearchResponse::new);
    }

    @Override
    public Writeable.Reader<AsyncSearchResponse> getResponseReader() {
        return AsyncSearchResponse::new;
    }

    public static class Request extends ActionRequest {
        private final String id;
        private final int lastVersion;
        private final TimeValue waitForCompletion;
        private TimeValue keepAlive = TimeValue.MINUS_ONE;

        /**
         * Create a new request
         * @param id The id of the search progress request.
         * @param waitForCompletion The minimum time that the request should wait before returning a partial result.
         * @param lastVersion The last version returned by a previous call.
         */
        public Request(String id, TimeValue waitForCompletion, int lastVersion) {
            this.id = id;
            this.waitForCompletion = waitForCompletion;
            this.lastVersion = lastVersion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.waitForCompletion = TimeValue.timeValueMillis(in.readLong());
            this.lastVersion = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeLong(waitForCompletion.millis());
            out.writeInt(lastVersion);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (keepAlive != TimeValue.MINUS_ONE && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
                validationException =
                    addValidationError("keep_alive must be greater than 1 minute, got:" + keepAlive.toString(), validationException);
            }
            return validationException;
        }

        public String getId() {
            return id;
        }

        /**
         * Return the version of the previously retrieved {@link AsyncSearchResponse}.
         * Partial hits and aggs are not included in the new response if they match this
         * version and the request returns {@link RestStatus#NOT_MODIFIED}.
         */
        public int getLastVersion() {
            return lastVersion;
        }

        /**
         * Return the minimum time that the request should wait for completion.
         */
        public TimeValue getWaitForCompletion() {
            return waitForCompletion;
        }

        public TimeValue getKeepAlive() {
            return keepAlive;
        }

        public void setKeepAlive(TimeValue keepAlive) {
            this.keepAlive = keepAlive;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return lastVersion == request.lastVersion &&
                Objects.equals(id, request.id) &&
                waitForCompletion.equals(request.waitForCompletion) &&
                keepAlive.equals(request.keepAlive);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, lastVersion, waitForCompletion, keepAlive);
        }
    }
}
