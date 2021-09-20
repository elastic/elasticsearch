/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class InternalAggregationsTests extends ESTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables()
    );

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        InternalAggregation.ReduceContextBuilder builder = InternalAggregationTestCase.emptyReduceContextBuilder();
        InternalAggregation.ReduceContext reduceContext = randomBoolean() ? builder.forFinalReduction() : builder.forPartialReduction();
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testNonFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0L
        );
        List<InternalAggregations> aggs = singletonList(InternalAggregations.from(Collections.singletonList(terms)));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, maxBucketReduceContext().forPartialReduction());
        assertEquals(1, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(1, reducedAggs.aggregations.size());
    }

    public void testFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            10,
            1,
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0L
        );

        InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(terms));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(
            Collections.singletonList(aggs),
            maxBucketReduceContext().forFinalReduction()
        );
        assertEquals(0, reducedAggs.getTopLevelPipelineAggregators().size());
        assertEquals(2, reducedAggs.aggregations.size());
    }

    private InternalAggregation.ReduceContextBuilder maxBucketReduceContext() {
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        PipelineAggregator.PipelineTree tree = new PipelineAggregator.PipelineTree(
            emptyMap(),
            singletonList(maxBucketPipelineAggregationBuilder.create())
        );
        return InternalAggregationTestCase.emptyReduceContextBuilder(tree);
    }

    public static InternalAggregations createTestInstance() throws Exception {
        return createTestInstance(randomPipelineTree());
    }

    public static InternalAggregations createTestInstance(PipelineAggregator.PipelineTree pipelineTree) throws Exception {
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
        return new InternalAggregations(aggsList, () -> pipelineTree);
    }

    private static PipelineAggregator.PipelineTree randomPipelineTree() {
        List<PipelineAggregator> topLevelPipelineAggs = new ArrayList<>();
        if (randomBoolean()) {
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator) new MaxBucketPipelineAggregationBuilder("name1", "bucket1").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator) new AvgBucketPipelineAggregationBuilder("name2", "bucket2").create());
            }
            if (randomBoolean()) {
                topLevelPipelineAggs.add((SiblingPipelineAggregator) new SumBucketPipelineAggregationBuilder("name3", "bucket3").create());
            }
        }
        return new PipelineAggregator.PipelineTree(emptyMap(), topLevelPipelineAggs);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, Version.CURRENT, 0);
    }

    public void testSerializedSize() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        assertThat(DelayableWriteable.getSerializedSize(aggregations), equalTo((long) serialize(aggregations, Version.CURRENT).length));
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, Version version, int iteration) throws IOException {
        BytesRef serializedAggs = serialize(aggregations, version);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(serializedAggs.bytes), registry)) {
            in.setVersion(version);
            InternalAggregations deserialized = InternalAggregations.readFrom(in);
            assertEquals(aggregations.aggregations, deserialized.aggregations);
            if (iteration < 2) {
                writeToAndReadFrom(deserialized, version, iteration + 1);
            }
        }
    }

    private BytesRef serialize(InternalAggregations aggs, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            aggs.writeTo(out);
            return out.bytes().toBytesRef();
        }
    }
}
