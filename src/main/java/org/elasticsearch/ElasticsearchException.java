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

package org.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.HasRestHeaders;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A base class for all elasticsearch exceptions.
 */
public class ElasticsearchException extends RuntimeException implements ToXContent {

    public static final String REST_EXCEPTION_SKIP_CAUSE = "rest.exception.skip_cause";

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message.
     *
     * @param msg the detail message
     */
    public ElasticsearchException(String msg) {
        super(msg);
    }

    /**
     * Construct a <code>ElasticsearchException</code> with the specified detail message
     * and nested exception.
     *
     * @param msg   the detail message
     * @param cause the nested exception
     */
    public ElasticsearchException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Returns the rest status code associated with this exception.
     */
    public RestStatus status() {
        Throwable cause = unwrapCause();
        if (cause == this) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        } else {
            return ExceptionsHelper.status(cause);
        }
    }

    /**
     * Unwraps the actual cause from the exception for cases when the exception is a
     * {@link ElasticsearchWrapperException}.
     *
     * @see org.elasticsearch.ExceptionsHelper#unwrapCause(Throwable)
     */
    public Throwable unwrapCause() {
        return ExceptionsHelper.unwrapCause(this);
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    public String getDetailedMessage() {
        if (getCause() != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(toString()).append("; ");
            if (getCause() instanceof ElasticsearchException) {
                sb.append(((ElasticsearchException) getCause()).getDetailedMessage());
            } else {
                sb.append(getCause());
            }
            return sb.toString();
        } else {
            return super.toString();
        }
    }


    /**
     * Retrieve the innermost cause of this exception, if none, returns the current exception.
     */
    public Throwable getRootCause() {
        Throwable rootCause = this;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

    /**
     * Check whether this exception contains an exception of the given type:
     * either it is of the given class itself or it contains a nested cause
     * of the given type.
     *
     * @param exType the exception type to look for
     * @return whether there is a nested exception of the specified type
     */
    public boolean contains(Class exType) {
        if (exType == null) {
            return false;
        }
        if (exType.isInstance(this)) {
            return true;
        }
        Throwable cause = getCause();
        if (cause == this) {
            return false;
        }
        if (cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).contains(exType);
        } else {
            while (cause != null) {
                if (exType.isInstance(cause)) {
                    return true;
                }
                if (cause.getCause() == cause) {
                    break;
                }
                cause = cause.getCause();
            }
            return false;
        }
    }

    /**
     * A base class for exceptions that should carry rest headers
     */
    @SuppressWarnings("unchecked")
    public static class WithRestHeaders extends ElasticsearchException implements HasRestHeaders {

        private final ImmutableMap<String, List<String>> headers;

        public WithRestHeaders(String msg, Tuple<String, String[]>... headers) {
            super(msg);
            this.headers = headers(headers);
        }

        @Override
        public ImmutableMap<String, List<String>> getHeaders() {
            return headers;
        }

        protected static Tuple<String, String[]> header(String name, String... values) {
            return Tuple.tuple(name, values);
        }

        private static ImmutableMap<String, List<String>> headers(Tuple<String, String[]>... headers) {
            Map<String, List<String>> map = Maps.newHashMap();
            for (Tuple<String, String[]> header : headers) {
                List<String> list = map.get(header.v1());
                if (list == null) {
                    list = Lists.newArrayList(header.v2());
                    map.put(header.v1(), list);
                } else {
                    for (String value : header.v2()) {
                        list.add(value);
                    }
                }
            }
            return ImmutableMap.copyOf(map);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (this instanceof ElasticsearchWrapperException) {
            toXContent(builder, params, this);
        } else {
            builder.field("type", getExceptionName(this));
            builder.field("reason", getMessage());
            innerToXContent(builder, params);
        }
        return builder;
    }

    /**
     * Renders additional per exception information into the xcontent
     */
    protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        causeToXContent(builder, params);
    }

    /**
     * Renders a cause exception as xcontent
     */
    protected final void causeToXContent(XContentBuilder builder, Params params) throws IOException {
        final Throwable cause = getCause();
        if (cause != null && params.paramAsBoolean(REST_EXCEPTION_SKIP_CAUSE, false) == false) {
            builder.field("caused_by");
            builder.startObject();
            toXContent(builder, params, cause);
            builder.endObject();
        }
    }

    /**
     * Statis toXContent helper method that also renders non {@link org.elasticsearch.ElasticsearchException} instances as XContent.
     */
    public static void toXContent(XContentBuilder builder, Params params, Throwable ex) throws IOException {
        ex = ExceptionsHelper.unwrapCause(ex);
        if (ex instanceof ElasticsearchException) {
            ((ElasticsearchException) ex).toXContent(builder, params);
        } else {
            builder.field("type", getExceptionName(ex));
            builder.field("reason", ex.getMessage());
            if (ex.getCause() != null) {
                builder.field("caused_by");
                builder.startObject();
                toXContent(builder, params, ex.getCause());
                builder.endObject();
            }
        }
    }

    /**
     * Returns the root cause of this exception or mupltiple if different shards caused different exceptions
     */
    public ElasticsearchException[] guessRootCauses() {
        final Throwable cause = getCause();
        if (cause != null && cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).guessRootCauses();
        }
        return new ElasticsearchException[] {this};
    }

    /**
     * Returns the root cause of this exception or mupltiple if different shards caused different exceptions.
     * If the given exception is not an instance of {@link org.elasticsearch.ElasticsearchException} an empty array
     * is returned.
     */
    public static ElasticsearchException[] guessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof ElasticsearchException) {
            return ((ElasticsearchException) ex).guessRootCauses();
        }
        return new ElasticsearchException[0];
    }

    /**
     * Returns a underscore case name for the given exception. This method strips <tt>Elasticsearch</tt> prefixes from exception names.
     */
    public static String getExceptionName(Throwable ex) {
        String simpleName = ex.getClass().getSimpleName();
        if (simpleName.startsWith("Elasticsearch")) {
            simpleName = simpleName.substring("Elasticsearch".length());
        }
        return Strings.toUnderscoreCase(simpleName);
    }

    @Override
    public String toString() {
        return ExceptionsHelper.detailedMessage(this).trim();
    }


}
