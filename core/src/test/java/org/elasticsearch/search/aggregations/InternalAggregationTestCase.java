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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public abstract class InternalAggregationTestCase<T extends InternalAggregation> extends AbstractWireSerializingTestCase<T> {
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());

    protected abstract T createTestInstance(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData);

    /** Return an instance on an unmapped field. */
    protected T createUnmappedInstance(String name,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        // For most impls, we use the same instance in the unmapped case and in the mapped case
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    public void testReduceRandom() {
        String name = randomAlphaOfLength(5);
        List<T> inputs = new ArrayList<>();
        List<InternalAggregation> toReduce = new ArrayList<>();
        int toReduceSize = between(1, 200);
        for (int i = 0; i < toReduceSize; i++) {
            T t = randomBoolean() ? createUnmappedInstance(name) : createTestInstance(name);
            inputs.add(t);
            toReduce.add(t);
        }
        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
        if (randomBoolean() && toReduce.size() > 1) {
            // sometimes do an incremental reduce
            Collections.shuffle(toReduce, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregation> internalAggregations = toReduce.subList(0, r);
            InternalAggregation.ReduceContext context =
                new InternalAggregation.ReduceContext(bigArrays, mockScriptService, false);
            @SuppressWarnings("unchecked")
            T reduced = (T) inputs.get(0).reduce(internalAggregations, context);
            toReduce = new ArrayList<>(toReduce.subList(r, toReduceSize));
            toReduce.add(reduced);
        }
        InternalAggregation.ReduceContext context =
            new InternalAggregation.ReduceContext(bigArrays, mockScriptService, true);
        @SuppressWarnings("unchecked")
        T reduced = (T) inputs.get(0).reduce(toReduce, context);
        assertReduced(reduced, inputs);
    }

    /**
     * overwrite in tests that need it
     */
    protected ScriptService mockScriptService() {
        return null;
    }

    protected abstract void assertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance() {
        return createTestInstance(randomAlphaOfLength(5));
    }

    private T createTestInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    /** Return an instance on an unmapped field. */
    protected final T createUnmappedInstance(String name) {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return createUnmappedInstance(name, pipelineAggregators, metaData);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
