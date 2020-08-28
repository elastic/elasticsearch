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

import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;

import java.util.stream.Stream;

/**
 * This class acts as a basic result collection that can be extended to do on-the-fly reduction or result processing
 */
class ArraySearchPhaseResults<Result extends SearchPhaseResult> extends SearchPhaseResults<Result> {
    final AtomicArray<Result> results;

    ArraySearchPhaseResults(int size) {
        super(size);
        this.results = new AtomicArray<>(size);
    }

    Stream<Result> getSuccessfulResults() {
        return results.asList().stream();
    }

    @Override
    void consumeResult(Result result, Runnable next) {
        assert results.get(result.getShardIndex()) == null : "shardIndex: " + result.getShardIndex() + " is already set";
        results.set(result.getShardIndex(), result);
        next.run();
    }

    boolean hasResult(int shardIndex) {
        return results.get(shardIndex) != null;
    }

    @Override
    AtomicArray<Result> getAtomicArray() {
        return results;
    }
}
