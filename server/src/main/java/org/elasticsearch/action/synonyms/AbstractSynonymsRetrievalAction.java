/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntFunction;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public abstract class AbstractSynonymsRetrievalAction<T extends ActionResponse> extends ActionType<T> {

    public AbstractSynonymsRetrievalAction(String name, Writeable.Reader<T> reader) {
        super(name, reader);
    }

    public static class Request extends ActionRequest {
        private static final int MAX_SYNONYMS_RESULTS = 10_000;
        private final int from;
        private final int size;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.from = in.readVInt();
            this.size = in.readVInt();
        }

        public Request(int from, int size) {
            this.from = from;
            this.size = size;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            validationException = validatePositiveInt("from", from, validationException);
            validationException = validatePositiveInt("size", size, validationException);

            if (from + size > MAX_SYNONYMS_RESULTS) {
                validationException = addValidationError(
                    "Too many synonyms to retrieve. [from] + [size] must be less than or equal to " + MAX_SYNONYMS_RESULTS,
                    validationException
                );
            }

            return validationException;
        }

        private static ActionRequestValidationException validatePositiveInt(
            String paramName,
            int value,
            ActionRequestValidationException validationException
        ) {
            if (value < 0) {
                validationException = addValidationError("[" + paramName + "] must be a positive integer", validationException);
            }

            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(from);
            out.writeVInt(size);
        }

        public int from() {
            return from;
        }

        public int size() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return from == request.from && size == request.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(from, size);
        }
    }

    public abstract static class AbstractPagedResultResponse<T extends Writeable> extends ActionResponse implements ToXContentObject {

        private final Writeable[] resultList;

        private final long totalCount;

        protected abstract String resultFieldName();

        protected abstract Reader<T> reader();

        protected abstract IntFunction<T[]> arraySupplier();

        public AbstractPagedResultResponse(StreamInput in) throws IOException {
            this.totalCount = in.readVLong();
            this.resultList = in.readArray(reader(), arraySupplier());
        }

        public AbstractPagedResultResponse(SynonymsManagementAPIService.PagedResult<T> result) {
            this.resultList = result.synonymRules();
            this.totalCount = result.totalSynonymRules();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("count", totalCount);
                builder.array(resultFieldName(), (Object[]) resultList);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(totalCount);
            out.writeArray(resultList);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            @SuppressWarnings("unchecked")
            AbstractPagedResultResponse<T> that = (AbstractPagedResultResponse<T>) o;
            return totalCount == that.totalCount && Arrays.equals(resultList, that.resultList);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(totalCount);
            result = 31 * result + Arrays.hashCode(resultList);
            return result;
        }
    }

}
