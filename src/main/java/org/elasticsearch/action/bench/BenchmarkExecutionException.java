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

package org.elasticsearch.action.bench;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Indicates a benchmark failure due to too many failures being encountered.
 */
public class BenchmarkExecutionException extends ElasticsearchException {

    private List<String> errorMessages = new ArrayList<>();

    public BenchmarkExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public BenchmarkExecutionException(String msg, List<String> errorMessages) {
        super(msg);
        this.errorMessages.addAll(errorMessages);
    }

    public List<String> errorMessages() {
        return errorMessages;
    }

    @Override
    public RestStatus status() {
        return RestStatus.INTERNAL_SERVER_ERROR;
    }
}
