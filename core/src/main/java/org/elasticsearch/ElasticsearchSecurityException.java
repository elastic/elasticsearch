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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Generic security exception
 */
public class ElasticsearchSecurityException extends ElasticsearchException {

    private final RestStatus status;

    public ElasticsearchSecurityException(String msg, RestStatus status, Throwable cause, Object... args) {
        super(msg, cause, args);
        this.status = status ;
    }

    public ElasticsearchSecurityException(String msg, Throwable cause, Object... args) {
        this(msg, ExceptionsHelper.status(cause), cause, args);
    }

    public ElasticsearchSecurityException(String msg, Object... args) {
        this(msg, RestStatus.INTERNAL_SERVER_ERROR, null, args);
    }

    public ElasticsearchSecurityException(String msg, RestStatus status, Object... args) {
        this(msg, status, null, args);
    }

    public ElasticsearchSecurityException(StreamInput in) throws IOException {
        super(in);
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        RestStatus.writeTo(out, status);
    }

    @Override
    public final RestStatus status() {
        return status;
    }
}
