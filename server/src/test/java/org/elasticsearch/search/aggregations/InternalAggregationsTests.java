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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsTests;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class InternalAggregationsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        InternalAggregation.ReduceContextBuilder builder = InternalAggregationTestCase.emptyReduceContextBuilder();
        InternalAggregation.ReduceContext reduceContext = randomBoolean() ? builder.forFinalReduction() : builder.forPartialReduction();
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testNonFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true),
            10, 1, Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);
        List<InternalAggregations> aggs = singletonList(InternalAggregations.from(Collections.singletonList(terms)));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, maxBucketReduceContext().forPartialReduction());
        assertEquals(1, reducedAggs.aggregations.size());
    }

    public void testFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true),
            10, 1, Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);

        InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(terms));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(Collections.singletonList(aggs),
                maxBucketReduceContext().forFinalReduction());
        assertEquals(2, reducedAggs.aggregations.size());
    }

    private InternalAggregation.ReduceContextBuilder maxBucketReduceContext() {
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        PipelineAggregator.PipelineTree tree =
                new PipelineAggregator.PipelineTree(emptyMap(), singletonList(maxBucketPipelineAggregationBuilder.create()));
        return InternalAggregationTestCase.emptyReduceContextBuilder(tree);
    }

    public static InternalAggregations createTestInstance() throws Exception {
        List<InternalAggregation> aggsList = new ArrayList<>();
        if (randomBoolean()) {
            StringTermsTests stringTermsTests = new StringTermsTests();
            stringTermsTests.init();
            stringTermsTests.setUp();
            aggsList.add(stringTermsTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalDateHistogramTests dateHistogramTests = new InternalDateHistogramTests();
            dateHistogramTests.setUp();
            aggsList.add(dateHistogramTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalSimpleValueTests simpleValueTests = new InternalSimpleValueTests();
            aggsList.add(simpleValueTests.createTestInstance());
        }
        return InternalAggregations.from(aggsList);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, 0);
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, int iteration) throws IOException {
        Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            aggregations.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytesRef().bytes), registry)) {
                in.setVersion(version);
                InternalAggregations deserialized = InternalAggregations.readFrom(in);
                assertEquals(aggregations.aggregations, deserialized.aggregations);
                if (iteration < 2) {
                    writeToAndReadFrom(deserialized, iteration + 1);
                }
            }
        }
    }
}
