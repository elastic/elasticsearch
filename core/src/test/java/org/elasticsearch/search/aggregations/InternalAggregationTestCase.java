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
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public abstract class InternalAggregationTestCase<T extends InternalAggregation> extends AbstractWireSerializingTestCase<T> {
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            new SearchModule(Settings.EMPTY, false, emptyList()).getNamedWriteables());

    protected abstract T createTestInstance(String name,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData);

    /** Return an instance on an unmapped field. */
    protected T createUnmappedInstance(String name,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        // For most impls, we use the same instance in the unmapped case and in the mapped case
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    public final void testReduceRandom() {
        List<T> inputs = new ArrayList<>();
        List<InternalAggregation> toReduce = new ArrayList<>();
        int toReduceSize = between(1, 200);
        for (int i = 0; i < toReduceSize; i++) {
            T t = randomBoolean() ? createUnmappedInstance() : createTestInstance();
            inputs.add(t);
            toReduce.add(t);
        }
        if (randomBoolean()) {
            // we leave at least one in the list
            List<InternalAggregation> internalAggregations = randomSubsetOf(randomIntBetween(1, toReduceSize), toReduce);
            InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(null, null, false);
            @SuppressWarnings("unchecked")
            T reduced = (T) inputs.get(0).reduce(internalAggregations, context);
            toReduce.removeAll(internalAggregations);
            toReduce.add(reduced);
        }
        InternalAggregation.ReduceContext context = new InternalAggregation.ReduceContext(null, null, true);
        @SuppressWarnings("unchecked")
        T reduced = (T) inputs.get(0).reduce(toReduce, context);
        assertReduced(reduced, inputs);
    }

    protected abstract void assertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance() {
        String name = randomAsciiOfLength(5);
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
        }
        return createTestInstance(name, pipelineAggregators, metaData);
    }

    /** Return an instance on an unmapped field. */
    protected final T createUnmappedInstance() {
        String name = randomAsciiOfLength(5);
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        // TODO populate pipelineAggregators
        Map<String, Object> metaData = new HashMap<>();
        int metaDataCount = randomBoolean() ? 0 : between(1, 10);
        while (metaData.size() < metaDataCount) {
            metaData.put(randomAsciiOfLength(5), randomAsciiOfLength(5));
        }
        return createUnmappedInstance(name, pipelineAggregators, metaData);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
