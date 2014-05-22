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
package org.elasticsearch.action.benchmark.start;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;

/**
 * Action for starting benchmarks.
 */
public class BenchmarkStartAction extends ClientAction<BenchmarkStartRequest, BenchmarkStartResponse, BenchmarkStartRequestBuilder> {

    public static final BenchmarkStartAction INSTANCE = new BenchmarkStartAction();
    public static final String NAME = "benchmark/start";

    private BenchmarkStartAction() {
        super(NAME);
    }

    @Override
    public BenchmarkStartResponse newResponse() {
        return new BenchmarkStartResponse();
    }

    @Override
    public BenchmarkStartRequestBuilder newRequestBuilder(Client client) {
        return new BenchmarkStartRequestBuilder(client, Strings.EMPTY_ARRAY);
    }
}
