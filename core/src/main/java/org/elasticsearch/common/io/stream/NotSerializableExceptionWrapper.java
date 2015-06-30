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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This exception can be used to wrap a given, not serializable exception
 * to serialize via {@link StreamOutput#writeThrowable(Throwable)}.
 * This class will perserve the stacktrace as well as the suppressed exceptions of
 * the throwable it was created with instead of it's own. The stacktrace has no indication
 * of where this exception was created.
 */
public final class NotSerializableExceptionWrapper extends ElasticsearchException.WithRestHeadersException {

    private final String name;

    public NotSerializableExceptionWrapper(Throwable other, Map<String, List<String>> headers) {
        super(other.getMessage(), other.getCause(), headers);
        this.name = ElasticsearchException.getExceptionName(other);
        setStackTrace(other.getStackTrace());
        for (Throwable otherSuppressed : other.getSuppressed()) {
            addSuppressed(otherSuppressed);
        }
    }

    public NotSerializableExceptionWrapper(WithRestHeadersException other) {
        this(other, other.getHeaders());
    }

    public NotSerializableExceptionWrapper(Throwable other) {
        this(other, Collections.EMPTY_MAP);
    }

    public NotSerializableExceptionWrapper(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }

    @Override
    protected String getExceptionName() {
        return name;
    }
}
