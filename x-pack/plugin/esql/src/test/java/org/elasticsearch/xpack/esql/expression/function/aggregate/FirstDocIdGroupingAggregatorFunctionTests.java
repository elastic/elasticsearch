/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.FirstDocIdGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
                    .build(DocVector.config().mayContainDuplicates());
                pages.add(new Page(docVector.asBlock(), groups.build().asBlock()));
            }
        }
        AggregatorMode aggregatorMode = AggregatorMode.INITIAL;
        var aggregatorFactory = new FirstDocIdGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            aggregatorMode,
            List.of(0)
        );
        HashAggregationOperator hashAggregationOperator = new HashAggregationOperator.Builder().mode(aggregatorMode)
            .aggregators(List.of(aggregatorFactory))
            .groups(List.of(new BlockHash.GroupSpec(1, ElementType.INT)))
            .aggregationBatchSize(randomIntBetween(1, 1024))
            .maxPageSize(Integer.MAX_VALUE)
            .partialEmit(randomIntBetween(SourceOperator.MIN_TARGET_PAGE_SIZE, SourceOperator.TARGET_PAGE_SIZE / 10), 1.0)
            .build()
            .get(driverContext);
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

    /**
     * Documents that left their node, like the rows a {@code FORK} merges on the coordinator, keep their origin. Every
     * input page brings its own dictionary of origins, as pages from different nodes do.
     */
    public void testDocumentsThatLeftTheirNodeKeepTheirOrigin() {
        List<DocOrigin> origins = origins(between(1, 5));
        Map<Integer, DetachedDoc> expectedFirstDocs = new HashMap<>();
        List<Page> pages = detachedPages(origins, expectedFirstDocs);

        Map<Integer, DetachedDoc> actualFirstDocs = new HashMap<>();
        for (Page out : runSingle(driverContext(), pages, Integer.MAX_VALUE)) {
            IntBlock groups = out.getBlock(0);
            DocVector docVector = ((DocBlock) out.getBlock(1)).asVector();
            for (int p = 0; p < out.getPositionCount(); p++) {
                DetachedDoc doc = new DetachedDoc(docVector.origin(p), docVector.segments().getInt(p), docVector.docs().getInt(p));
                assertNull(actualFirstDocs.put(groups.getInt(p), doc));
            }
            out.close();
        }
        assertThat(actualFirstDocs, Matchers.equalTo(expectedFirstDocs));
    }

    /**
     * An output page names only the origins of its own rows, so a page doesn't carry every origin the aggregation saw.
     */
    public void testAnOutputPageCarriesOnlyTheOriginsOfItsRows() {
        List<Page> pages = detachedPages(origins(between(2, 5)), new HashMap<>());

        for (Page out : runSingle(driverContext(), pages, between(1, 10))) {
            DocVector docVector = ((DocBlock) out.getBlock(1)).asVector();
            Set<DocOrigin> referenced = new HashSet<>();
            for (int p = 0; p < out.getPositionCount(); p++) {
                referenced.add(docVector.origin(p));
            }
            assertThat(docVector.origins().size(), Matchers.equalTo(referenced.size()));
            out.close();
        }
    }

    /**
     * A breaker that trips while the aggregation numbers the origins of its documents, or while it builds an output page,
     * leaves nothing on any breaker. {@link ComputeTestCase} checks the breakers after the test.
     */
    public void testDocumentsThatLeftTheirNodeReleaseEverythingWhenTheBreakerTrips() {
        List<Page> pages = detachedPages(origins(between(1, 5)), new HashMap<>());
        BlockFactory cranky = crankyBlockFactory();

        try {
            List<Page> out = runSingle(new DriverContext(cranky.bigArrays(), cranky, null), pages, between(1, 10));
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(out.iterator(), p -> p::releaseBlocks)));
        } catch (CircuitBreakingException e) {
            assertThat(e.getMessage(), Matchers.equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
    }

    /**
     * A vector holds either documents of this node or documents that left their node, so one aggregation can't take both,
     * whichever kind comes first.
     */
    public void testDocumentsOfThisNodeAndDocumentsThatLeftTheirNodeDontMix() {
        Map<Integer, RefCounted> shardRefs = Map.of(0, AbstractRefCounted.of(() -> {}));
        Page attached;
        try (var docs = DocVector.newFixedBuilder(blockFactory(), 1)) {
            docs.append(0, 0, 0);
            DocVector docVector = docs.shardRefCounters(new FirstDocIdGroupingAggregatorFunction.MappedShardRefs<>(shardRefs))
                .build(DocVector.config());
            attached = new Page(docVector.asBlock());
        }
        Page detached;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory(), 1)) {
            builder.appendShard(0).appendSegment(0).appendDoc(0);
            detached = new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins(1)))));
        }
        boolean attachedFirst = randomBoolean();
        Page first = attachedFirst ? attached : detached;
        Page second = attachedFirst ? detached : attached;
        try (var aggregator = new FirstDocIdGroupingAggregatorFunction(List.of(0), driverContext())) {
            aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), first).close();
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> aggregator.prepareProcessRawInputPage(new SeenGroupIds.Empty(), second)
            );
            assertThat(
                e.getMessage(),
                Matchers.equalTo("first_doc_id can't mix documents of this node with documents that left their node")
            );
        } finally {
            attached.releaseBlocks();
            detached.releaseBlocks();
            shardRefs.get(0).decRef();
        }
    }

    /**
     * Pages of documents that left their node, and the first document of each group in {@code expectedFirstDocs}. The
     * first page references every origin, in a random order, and the others a random dictionary drawn from {@code origins}.
     */
    private List<Page> detachedPages(List<DocOrigin> origins, Map<Integer, DetachedDoc> expectedFirstDocs) {
        int numPages = between(1, 20);
        List<Page> pages = new ArrayList<>(numPages);
        for (int i = 0; i < numPages; i++) {
            List<DocOrigin> pageOrigins = i == 0 ? shuffledList(origins) : randomNonEmptySubsetOf(origins).stream().toList();
            int positions = between(pageOrigins.size(), 200);
            try (
                DocBlock.Builder docs = DocBlock.newBlockBuilder(blockFactory(), positions);
                var groups = blockFactory().newIntVectorFixedBuilder(positions)
            ) {
                for (int p = 0; p < positions; p++) {
                    int ordinal = p < pageOrigins.size() ? p : between(0, pageOrigins.size() - 1);
                    int segment = between(0, 5);
                    int docId = randomNonNegativeInt();
                    docs.appendShard(ordinal).appendSegment(segment).appendDoc(docId);
                    int group = between(0, 100);
                    groups.appendInt(group);
                    expectedFirstDocs.putIfAbsent(group, new DetachedDoc(pageOrigins.get(ordinal), segment, docId));
                }
                DocBlock docBlock = docs.build(DocVector.config().origins(DocOrigins.of(pageOrigins)).mayContainDuplicates());
                pages.add(new Page(docBlock, groups.build().asBlock()));
            }
        }
        return pages;
    }

    /**
     * Groups {@code pages} by their second column in a single pass, as the coordinator aggregates the rows of a {@code FORK}.
     * Releases the pages when the aggregation fails to build.
     */
    private List<Page> runSingle(DriverContext driverContext, List<Page> pages, int maxPageSize) {
        var aggregatorFactory = new FirstDocIdGroupingAggregatorFunction.FunctionSupplier().groupingAggregatorFactory(
            AggregatorMode.SINGLE,
            List.of(0)
        );
        Operator aggregation;
        try {
            aggregation = new HashAggregationOperator.Builder().mode(AggregatorMode.SINGLE)
                .aggregators(List.of(aggregatorFactory))
                .groups(List.of(new BlockHash.GroupSpec(1, ElementType.INT)))
                .aggregationBatchSize(randomIntBetween(1, 1024))
                .maxPageSize(maxPageSize)
                .build()
                .get(driverContext);
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(Releasables.wrap(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks)));
            throw e;
        }
        return new TestDriverRunner().builder(driverContext).input(pages).run(aggregation);
    }

    private static List<DocOrigin> origins(int count) {
        List<DocOrigin> origins = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            origins.add(new DocOrigin("", "node-" + i, new ShardId("idx", "uuid", i), new ShardSearchContextId("session", i)));
        }
        return origins;
    }

    record Doc(int shard, int segment, int docId) {

    }

    record DetachedDoc(DocOrigin origin, int segment, int docId) {}
}
