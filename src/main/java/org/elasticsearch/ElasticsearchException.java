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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.HasRestHeaders;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;

/**
 * A base class for all elasticsearch exceptions.
 */
public class ElasticsearchException extends RuntimeException {

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
        } else if (cause instanceof ElasticsearchException) {
            return ((ElasticsearchException) cause).status();
        } else if (cause instanceof IllegalArgumentException) {
            return RestStatus.BAD_REQUEST;
        } else {
            return RestStatus.INTERNAL_SERVER_ERROR;
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
     * Retrieve the most specific cause of this exception, that is,
     * either the innermost cause (root cause) or this exception itself.
     * <p>Differs from {@link #getRootCause()} in that it falls back
     * to the present exception if there is no root cause.
     *
     * @return the most specific cause (never <code>null</code>)
     */
    public Throwable getMostSpecificCause() {
        Throwable rootCause = getRootCause();
        return (rootCause != null ? rootCause : this);
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

        public WithRestHeaders(String msg, @Nullable ImmutableMap<String, List<String>> headers) {
            super(msg);
            this.headers = headers != null ? headers : ImmutableMap.<String, List<String>>of();
        }

        public WithRestHeaders(String msg, Throwable cause, Tuple<String, String[]>... headers) {
            super(msg, cause);
            this.headers = headers(headers);
        }

        public WithRestHeaders(String msg, Throwable cause, @Nullable ImmutableMap<String, List<String>> headers) {
            super(msg, cause);
            this.headers = headers != null ? headers : ImmutableMap.<String, List<String>>of();
        }

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
}
