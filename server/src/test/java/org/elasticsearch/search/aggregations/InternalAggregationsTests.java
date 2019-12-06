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
import org.elasticsearch.search.aggregations.pipeline.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValueTests;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalAggregationsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        InternalAggregation.ReduceContext reduceContext = new InternalAggregation.ReduceContext(null, null, randomBoolean());
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testNonFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true),
            10, 1, Collections.emptyList(), Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);
        List<SiblingPipelineAggregator> topLevelPipelineAggs = new ArrayList<>();
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        topLevelPipelineAggs.add((SiblingPipelineAggregator)maxBucketPipelineAggregationBuilder.create());
        List<InternalAggregations> aggs = Collections.singletonList(new InternalAggregations(Collections.singletonList(terms),
            topLevelPipelineAggs));
        InternalAggregation.ReduceContext reduceContext = new InternalAggregation.ReduceContext(null, null, false);
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, reduceContext);
        assertEquals(1, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(1, reducedAggs.aggregations.size());
    }

    public void testFinalReduceTopLevelPipelineAggs()  {
        InternalAggregation terms = new StringTerms("name", BucketOrder.key(true),
            10, 1, Collections.emptyList(), Collections.emptyMap(), DocValueFormat.RAW, 25, false, 10, Collections.emptyList(), 0);

        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        SiblingPipelineAggregator siblingPipelineAggregator = (SiblingPipelineAggregator) maxBucketPipelineAggregationBuilder.create();
        InternalAggregation.ReduceContext reduceContext = new InternalAggregation.ReduceContext(null, null, true);
        final InternalAggregations reducedAggs;
        if (randomBoolean()) {
            InternalAggregations aggs = new InternalAggregations(Collections.singletonList(terms),
                Collections.singletonList(siblingPipelineAggregator));
            reducedAggs = InternalAggregations.topLevelReduce(Collections.singletonList(aggs), reduceContext);
        } else {
            InternalAggregations aggs = new InternalAggregations(Collections.singletonList(terms),
                Collections.singletonList(siblingPipelineAggregator));
            reducedAggs = InternalAggregations.topLevelReduce(Collections.singletonList(aggs), reduceContext);
        }
        assertEquals(0, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(2, reducedAggs.aggregations.size());
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
        List<SiblingPipelineAggregator> topLevelPipelineAggs = new ArrayList<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new MaxBucketPipelineAggregationBuilder("name1", "bucket1").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new AvgBucketPipelineAggregationBuilder("name2", "bucket2").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator)new SumBucketPipelineAggregationBuilder("name3", "bucket3").create());
            }
        }
        return new InternalAggregations(aggsList, topLevelPipelineAggs);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, 0);
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, int iteration) throws IOException {
        Version version = VersionUtils.randomVersion(random());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            aggregations.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes().toBytesRef().bytes), registry)) {
                in.setVersion(version);
                InternalAggregations deserialized = new InternalAggregations(in);
                assertEquals(aggregations.aggregations, deserialized.aggregations);
                if (aggregations.getTopLevelPipelineAggregators() == null) {
                    assertEquals(0, deserialized.getTopLevelPipelineAggregators().size());
                } else {
                    assertEquals(aggregations.getTopLevelPipelineAggregators().size(),
                        deserialized.getTopLevelPipelineAggregators().size());
                    for (int i = 0; i < aggregations.getTopLevelPipelineAggregators().size(); i++) {
                        SiblingPipelineAggregator siblingPipelineAggregator1 = aggregations.getTopLevelPipelineAggregators().get(i);
                        SiblingPipelineAggregator siblingPipelineAggregator2 = deserialized.getTopLevelPipelineAggregators().get(i);
                        assertArrayEquals(siblingPipelineAggregator1.bucketsPaths(), siblingPipelineAggregator2.bucketsPaths());
                        assertEquals(siblingPipelineAggregator1.name(), siblingPipelineAggregator2.name());
                    }
                }
                if (iteration < 2) {
                    //serialize this enough times to make sure that we are able to write again what we read
                    writeToAndReadFrom(deserialized, iteration + 1);
                }
            }
        }
    }
}
