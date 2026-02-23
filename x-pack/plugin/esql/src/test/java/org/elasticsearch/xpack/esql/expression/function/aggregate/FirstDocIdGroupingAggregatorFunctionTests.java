/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FirstDocIdGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstDocIdGroupingAggregatorFunctionTests extends ComputeTestCase {

    protected final DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    public void testSimple() {
        DriverContext driverContext = driverContext();
        int numPages = between(1, 100);
        List<Page> pages = new ArrayList<>(numPages);
        Map<Integer, Doc> expectedFirstDocs = new HashMap<>();
        Map<Integer, RefCounted> shardRefs = new HashMap<>();
        for (int i = 0; i < numPages; i++) {
            int positions = between(1, 1000);
            try (
                var docs = DocVector.newFixedBuilder(blockFactory(), positions);
                var groups = blockFactory().newIntVectorFixedBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    Doc doc = new Doc(between(0, 2), between(0, 5), randomNonNegativeInt());
                    shardRefs.putIfAbsent(doc.shard, AbstractRefCounted.of(() -> {}));
                    docs.append(doc.shard, doc.segment, doc.docId);
                    int group = between(0, 1000);
                    groups.appendInt(group);
                    expectedFirstDocs.putIfAbsent(group, doc);
                }
                DocVector docVector = docs.shardRefCounters(new FirstDocIdGroupingAggregatorFunction.MappedShardRefs<>(shardRefs))
                    .build(DocVector.config());
                pages.add(new Page(docVector.asBlock(), groups.build().asBlock()));
            }
        }
        AggregatorMode aggregatorMode = AggregatorMode.INITIAL;
        var aggregatorFactory = new FirstDocIdGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            aggregatorMode,
            List.of(0)
        );
        HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
            aggregatorMode,
            List.of(aggregatorFactory),
            () -> BlockHash.build(
                List.of(new BlockHash.GroupSpec(1, ElementType.INT)),
                driverContext.blockFactory(),
                randomIntBetween(1, 1024),
                randomBoolean()
            ),
            Integer.MAX_VALUE,
            1.0,
            driverContext
        );
        List<Page> outputPages = new ArrayList<>();
        Driver driver = TestDriverFactory.create(
            driverContext,
            new CannedSourceOperator(pages.iterator()),
            List.of(hashAggregationOperator),
            new PageConsumerOperator(outputPages::add)
        );
        new TestDriverRunner().run(driver);
        for (RefCounted value : shardRefs.values()) {
            value.decRef();
        }
        Map<Integer, Doc> actualFirstDocs = new HashMap<>();
        for (Page out : outputPages) {
            IntBlock groups = out.getBlock(0);
            DocVector docVector = ((DocBlock) out.getBlock(1)).asVector();
            for (int p = 0; p < out.getPositionCount(); p++) {
                Doc doc = new Doc(docVector.shards().getInt(p), docVector.segments().getInt(p), docVector.docs().getInt(p));
                int group = groups.getInt(p);
                assertNull(actualFirstDocs.put(group, doc));
            }
            out.close();
        }
        for (RefCounted value : shardRefs.values()) {
            assertFalse(value.hasReferences());
        }
        assertThat(actualFirstDocs, Matchers.equalTo(expectedFirstDocs));
    }

    record Doc(int shard, int segment, int docId) {

    }
}
