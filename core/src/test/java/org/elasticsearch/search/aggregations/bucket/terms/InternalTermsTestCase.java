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

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class InternalTermsTestCase extends InternalMultiBucketAggregationTestCase<InternalTerms<?, ?>> {

    private boolean showDocCount;
    private long docCountError;

    @Before
    public void init() {
        showDocCount = randomBoolean();
        docCountError = showDocCount ? randomInt(1000) : -1;
    }

    @Override
    protected InternalTerms<?, ?> createTestInstance(String name,
                                                     List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData,
                                                     InternalAggregations aggregations) {
        return createTestInstance(name, pipelineAggregators, metaData, aggregations, showDocCount, docCountError);
    }

    protected abstract InternalTerms<?, ?> createTestInstance(String name,
                                                              List<PipelineAggregator> pipelineAggregators,
                                                              Map<String, Object> metaData,
                                                              InternalAggregations aggregations,
                                                              boolean showTermDocCountError,
                                                              long docCountError);
}
