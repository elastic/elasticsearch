/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.aggregation.GroupingKey;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.HashAggregationOperator;
import org.elasticsearch.compute.operator.LocalSourceOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumerOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.compute.aggregation.AggregatorMode.FINAL;
import static org.elasticsearch.compute.aggregation.AggregatorMode.INITIAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CategorizeOperatorTests extends ESTestCase {
    public void testCategorization() {
        DriverContext driverContext = driverContext();
        LocalSourceOperator.BlockSupplier input = () -> {
            try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(10)) {
                builder.appendBytesRef(new BytesRef("words words words hello nik"));
                builder.appendBytesRef(new BytesRef("words words words goodbye nik"));
                builder.appendBytesRef(new BytesRef("words words words hello jan"));
                builder.appendBytesRef(new BytesRef("words words words goodbye jan"));
                builder.appendBytesRef(new BytesRef("words words words hello kitty"));
                builder.appendBytesRef(new BytesRef("words words words goodbye blue sky"));
                return new Block[] { builder.build().asBlock() };
            }
        };
        List<Page> output = new ArrayList<>();
        try {
            List<Operator> operators = new ArrayList<>();

            Categorize cat = new Categorize(Source.EMPTY, AbstractFunctionTestCase.field("f", DataType.KEYWORD));
            GroupingKey.Supplier key = cat.groupingKey(AbstractFunctionTestCase::evaluator);

            operators.add(
                new HashAggregationOperator.HashAggregationOperatorFactory(List.of(key.get(INITIAL)), List.of(), 16 * 1024).get(
                    driverContext
                )
            );
            operators.add(
                new HashAggregationOperator.HashAggregationOperatorFactory(List.of(key.get(FINAL)), List.of(), 16 * 1024).get(driverContext)
            );
            operators.add(
                new TopNOperator(
                    driverContext.blockFactory(),
                    driverContext.breaker(),
                    3,
                    List.of(ElementType.BYTES_REF),
                    List.of(TopNEncoder.UTF8),
                    List.of(new TopNOperator.SortOrder(0, true, true)),
                    16 * 1024
                )
            );

            Driver driver = new Driver(
                driverContext,
                new LocalSourceOperator(input),
                operators,
                new PageConsumerOperator(output::add),
                () -> {}
            );
            runDriver(driver);

            assertThat(output, hasSize(1));
            assertThat(output.get(0).getBlockCount(), equalTo(1));
            BytesRefBlock block = output.get(0).getBlock(0);
            BytesRefVector vector = block.asVector();
            List<String> values = new ArrayList<>();
            for (int p = 0; p < vector.getPositionCount(); p++) {
                values.add(vector.getBytesRef(p, new BytesRef()).utf8ToString());
            }
            assertThat(
                values,
                equalTo(
                    List.of(
                        ".*?words.+?words.+?words.+?goodbye.*?",
                        ".*?words.+?words.+?words.+?goodbye.+?blue.+?sky.*?",
                        ".*?words.+?words.+?words.+?hello.*?"
                    )
                )
            );
        } finally {
            Releasables.close(() -> Iterators.map(output.iterator(), (Page p) -> p::releaseBlocks));
        }
    }

    /**
     * {@link SourceOperator} that returns a sequence of pre-built {@link Page}s.
     * TODO: this class is copy-pasted from the esql:compute plugin; fix that
     */
    public static class CannedSourceOperator extends SourceOperator {

        private final Iterator<Page> page;

        public CannedSourceOperator(Iterator<Page> page) {
            this.page = page;
        }

        @Override
        public void finish() {
            while (page.hasNext()) {
                page.next();
            }
        }

        @Override
        public boolean isFinished() {
            return false == page.hasNext();
        }

        @Override
        public Page getOutput() {
            return page.next();
        }

        @Override
        public void close() {
            // release pages in the case of early termination - failure
            while (page.hasNext()) {
                page.next().releaseBlocks();
            }
        }
    }

    public void testCategorization_multipleNodes() {
        DriverContext driverContext = driverContext();
        LocalSourceOperator.BlockSupplier input1 = () -> {
            try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(10)) {
                builder.appendBytesRef(new BytesRef("a"));
                builder.appendBytesRef(new BytesRef("b"));
                builder.appendBytesRef(new BytesRef("words words words goodbye jan"));
                builder.appendBytesRef(new BytesRef("words words words goodbye nik"));
                builder.appendBytesRef(new BytesRef("words words words hello jan"));
                builder.appendBytesRef(new BytesRef("c"));
                return new Block[] { builder.build().asBlock() };
            }
        };

        LocalSourceOperator.BlockSupplier input2 = () -> {
            try (BytesRefVector.Builder builder = driverContext.blockFactory().newBytesRefVectorBuilder(10)) {
                builder.appendBytesRef(new BytesRef("words words words hello nik"));
                builder.appendBytesRef(new BytesRef("c"));
                builder.appendBytesRef(new BytesRef("words words words goodbye chris"));
                builder.appendBytesRef(new BytesRef("d"));
                builder.appendBytesRef(new BytesRef("e"));
                return new Block[] { builder.build().asBlock() };
            }
        };

        List<Page> intermediateOutput = new ArrayList<>();
        List<Page> finalOutput = new ArrayList<>();

        try {
            Categorize cat = new Categorize(Source.EMPTY, AbstractFunctionTestCase.field("f", DataType.KEYWORD));
            GroupingKey.Supplier key = cat.groupingKey(AbstractFunctionTestCase::evaluator);

            Driver driver = new Driver(
                driverContext,
                new LocalSourceOperator(input1),
                List.of(
                    new HashAggregationOperator.HashAggregationOperatorFactory(List.of(key.get(INITIAL)), List.of(), 16 * 1024).get(
                        driverContext
                    )
                ),
                new PageConsumerOperator(intermediateOutput::add),
                () -> {}
            );
            runDriver(driver);

            driver = new Driver(
                driverContext,
                new LocalSourceOperator(input2),
                List.of(
                    new HashAggregationOperator.HashAggregationOperatorFactory(List.of(key.get(INITIAL)), List.of(), 16 * 1024).get(
                        driverContext
                    )
                ),
                new PageConsumerOperator(intermediateOutput::add),
                () -> {}
            );
            runDriver(driver);

            assertThat(intermediateOutput, hasSize(2));

            driver = new Driver(
                driverContext,
                new CannedSourceOperator(intermediateOutput.iterator()),
                List.of(
                    new HashAggregationOperator.HashAggregationOperatorFactory(List.of(key.get(FINAL)), List.of(), 16 * 1024).get(
                        driverContext
                    ),
                    new TopNOperator(
                        driverContext.blockFactory(),
                        driverContext.breaker(),
                        10,
                        List.of(ElementType.BYTES_REF),
                        List.of(TopNEncoder.UTF8),
                        List.of(new TopNOperator.SortOrder(0, true, true)),
                        16 * 1024
                    )
                ),
                new PageConsumerOperator(finalOutput::add),
                () -> {}
            );
            runDriver(driver);

            assertThat(finalOutput, hasSize(1));
            assertThat(finalOutput.get(0).getBlockCount(), equalTo(1));
            BytesRefBlock block = finalOutput.get(0).getBlock(0);
            BytesRefVector vector = block.asVector();
            List<String> values = new ArrayList<>();
            for (int p = 0; p < vector.getPositionCount(); p++) {
                values.add(vector.getBytesRef(p, new BytesRef()).utf8ToString());
            }
            assertThat(
                values,
                equalTo(
                    List.of(
                        ".*?a.*?",
                        ".*?b.*?",
                        ".*?c.*?",
                        ".*?d.*?",
                        ".*?e.*?",
                        ".*?words.+?words.+?words.+?goodbye.*?",
                        ".*?words.+?words.+?words.+?hello.*?"
                    )
                )
            );
        } finally {
            Releasables.close(() -> Iterators.map(finalOutput.iterator(), (Page p) -> p::releaseBlocks));
        }
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    public static void runDriver(Driver driver) {
        ThreadPool threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(Settings.EMPTY, "esql", 1, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        var driverRunner = new DriverRunner(threadPool.getThreadContext()) {
            @Override
            protected void start(Driver driver, ActionListener<Void> driverListener) {
                Driver.start(threadPool.getThreadContext(), threadPool.executor("esql"), driver, between(1, 10000), driverListener);
            }
        };
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try {
            driverRunner.runToCompletion(List.of(driver), future);
            future.actionGet(TimeValue.timeValueSeconds(30));
        } finally {
            terminate(threadPool);
        }
    }
}
