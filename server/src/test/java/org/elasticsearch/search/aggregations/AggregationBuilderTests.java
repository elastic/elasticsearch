/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AggregationBuilderTests extends ESTestCase {

    public void testDeepCopy() {
        AggregationBuilder original = createTestInstance();
        // a copy that has metadata mutated
        AggregationBuilder copy = AggregationBuilder.deepCopy(original, levelCopy -> {
            AggregationBuilder returnCopy;
            // maybe return a different instance
            if (randomBoolean()) {
                returnCopy = levelCopy.shallowCopy(levelCopy.factoriesBuilder, levelCopy.getMetadata());
            } else {
                returnCopy = levelCopy;
            }
            // mutate "copied" instance in some way
            Map<String, Object> newMetadata = new HashMap<>(returnCopy.getMetadata() == null ? Map.of() : returnCopy.getMetadata());
            newMetadata.put("mutate", true);
            returnCopy.setMetadata(newMetadata);
            return returnCopy;
        });
        assertAggregationBuilderRecursively(original, copy, (levelOriginal, levelCopy, level) -> {
            // names are equal
            assertThat(levelOriginal.getName(), is(levelCopy.getName()));
            // pipeline aggs are equal
            assertThat(levelOriginal.getPipelineAggregations().size(), is(levelCopy.getPipelineAggregations().size()));
            var levelOriginalPipelineIterator = levelOriginal.getPipelineAggregations().iterator();
            var levelCopyPipelineIterator = levelCopy.getPipelineAggregations().iterator();
            while (levelOriginalPipelineIterator.hasNext() && levelCopyPipelineIterator.hasNext()) {
                assertThat(levelOriginalPipelineIterator.next(), is(levelCopyPipelineIterator.next()));
            }
            // metadata is altered
            if (levelOriginal.getMetadata() == null) {
                assertThat(levelCopy.getMetadata(), is(Map.of("mutate", true)));
            } else {
                assertThat(levelCopy.getMetadata(), is(Map.of("mutate", true, "level", level)));
            }
            assertThat(levelOriginal.getSubAggregations().size(), is(levelCopy.getSubAggregations().size()));
        });
    }

    private void assertAggregationBuilderRecursively(
        AggregationBuilder aggregationBuilder1,
        AggregationBuilder aggregationBuilder2,
        TriConsumer<AggregationBuilder, AggregationBuilder, Integer> comparator
    ) {
        assertAggregationBuilderRecursively(aggregationBuilder1, aggregationBuilder2, 0, comparator);
    }

    private void assertAggregationBuilderRecursively(
        AggregationBuilder aggregationBuilder1,
        AggregationBuilder aggregationBuilder2,
        Integer level,
        TriConsumer<AggregationBuilder, AggregationBuilder, Integer> comparator
    ) {
        comparator.apply(aggregationBuilder1, aggregationBuilder2, level);
        assertThat(aggregationBuilder1.getSubAggregations().size(), is(aggregationBuilder2.getSubAggregations().size()));
        Iterator<AggregationBuilder> agg1Iterator = aggregationBuilder1.getSubAggregations().iterator();
        Iterator<AggregationBuilder> agg2Iterator = aggregationBuilder2.getSubAggregations().iterator();
        while (agg1Iterator.hasNext() && agg2Iterator.hasNext()) {
            assertAggregationBuilderRecursively(agg1Iterator.next(), agg2Iterator.next(), level + 1, comparator);
        }
    }

    private TestAggregationBuilder createTestInstance() {
        return createTestInstance(0, randomIntBetween(0, 3), 0);
    }

    private TestAggregationBuilder createTestInstance(int level, int maxLevel, int idx) {
        TestAggregationBuilder levelAgg = new TestAggregationBuilder("level:" + level + "/" + idx);
        int nSubAgg = level < maxLevel ? randomIntBetween(1, 4) : 0;
        for (int i = 0; i < nSubAgg; i++) {
            levelAgg.subAggregation(createTestInstance(level + 1, maxLevel, i));
        }
        int nSubPipelineAgg = randomIntBetween(0, 3);
        for (int i = 0; i < nSubPipelineAgg; i++) {
            levelAgg.subAggregation(new TestPipelineAggregationBuilder("level:" + level + "/" + i));
        }
        if (randomBoolean()) {
            levelAgg.setMetadata(Map.of("level", level));
        }
        return levelAgg;
    }

    private static class TestAggregationBuilder extends AbstractAggregationBuilder<TestAggregationBuilder> {

        TestAggregationBuilder(String name) {
            super(name);
        }

        TestAggregationBuilder(TestAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            super(clone, factoriesBuilder, metadata);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder
        ) {
            throw new IllegalStateException("build not required for these tests");
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new TestAggregationBuilder(this, factoriesBuilder, metadata);
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return randomFrom(BucketCardinality.values());
        }

        @Override
        public String getType() {
            return "test";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }
    }

    private static class TestPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<TestPipelineAggregationBuilder> {

        TestPipelineAggregationBuilder(String name) {
            super(name, "test", new String[0]);
        }

        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metadata) {
            return null;
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return null;
        }

        @Override
        protected void validate(ValidationContext context) {}

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ZERO;
        }
    }
}
