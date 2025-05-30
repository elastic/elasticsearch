/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class InferenceOperatorTestCase<InferenceResultsType extends InferenceServiceResults> extends OperatorTestCase {
    private ThreadPool threadPool;

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool(
            getTestClass().getSimpleName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME,
                between(1, 10),
                1024,
                "esql",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    protected ThreadPool threadPool() {
        return threadPool;
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int minSize = Integer.min(1, size / 10);
        return new AbstractBlockSourceOperator(blockFactory, between(minSize, 8 * 1024)) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                try (var builder = blockFactory.newBytesRefVectorBuilder(length)) {
                    for (int i = 0; i < length; i++) {
                        builder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                    }
                    currentPosition += length;
                    return new Page(builder.build().asBlock());
                }
            }
        };
    }

    @Override
    public void testOperatorStatus() {
        DriverContext driverContext = driverContext();
        try (var operator = simple().get(driverContext)) {
            AsyncOperator.Status status = asInstanceOf(AsyncOperator.Status.class, operator.status());

            assertThat(status, notNullValue());
            assertThat(status.receivedPages(), equalTo(0L));
            assertThat(status.completedPages(), equalTo(0L));
            assertThat(status.procesNanos(), greaterThanOrEqualTo(0L));
        }
    }

    @SuppressWarnings("unchecked")
    protected InferenceRunner mockedSimpleInferenceRunner() {
        Client client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (action == InferenceAction.INSTANCE && request instanceof InferenceAction.Request inferenceRequest) {
                    InferenceAction.Response inferenceResponse = new InferenceAction.Response(mockInferenceResult(inferenceRequest));
                    listener.onResponse((Response) inferenceResponse);
                    return;
                }

                fail("Unexpected call to action [" + action.name() + "]");
            }
        };

        return new InferenceRunner(client, threadPool);
    }

    protected abstract InferenceResultsType mockInferenceResult(InferenceAction.Request request);

    protected void assertBlockContentEquals(Block input, Block result) {
        BytesRef scratch = new BytesRef();
        switch (input.elementType()) {
            case BOOLEAN -> assertBlockContentEquals(input, result, BooleanBlock::getBoolean, BooleanBlock.class);
            case INT -> assertBlockContentEquals(input, result, IntBlock::getInt, IntBlock.class);
            case LONG -> assertBlockContentEquals(input, result, LongBlock::getLong, LongBlock.class);
            case FLOAT -> assertBlockContentEquals(input, result, FloatBlock::getFloat, FloatBlock.class);
            case DOUBLE -> assertBlockContentEquals(input, result, DoubleBlock::getDouble, DoubleBlock.class);
            case BYTES_REF -> assertByteRefsBlockContentEquals(input, result, scratch);
            default -> throw new AssertionError(LoggerMessageFormat.format("Unexpected block type {}", input.elementType()));
        }
    }

    private <V extends Block, U> void assertBlockContentEquals(
        Block input,
        Block result,
        BiFunction<V, Integer, U> valueReader,
        Class<V> blockClass
    ) {
        V inputBlock = asInstanceOf(blockClass, input);
        V resultBlock = asInstanceOf(blockClass, result);

        assertAllPositions(inputBlock, (pos) -> {
            if (inputBlock.isNull(pos)) {
                assertThat(resultBlock.isNull(pos), equalTo(inputBlock.isNull(pos)));
            } else {
                assertThat(resultBlock.getValueCount(pos), equalTo(inputBlock.getValueCount(pos)));
                assertThat(resultBlock.getFirstValueIndex(pos), equalTo(inputBlock.getFirstValueIndex(pos)));
                for (int i = 0; i < inputBlock.getValueCount(pos); i++) {
                    assertThat(
                        valueReader.apply(resultBlock, resultBlock.getFirstValueIndex(pos) + i),
                        equalTo(valueReader.apply(inputBlock, inputBlock.getFirstValueIndex(pos) + i))
                    );
                }
            }
        });
    }

    private void assertAllPositions(Block block, Consumer<Integer> consumer) {
        for (int pos = 0; pos < block.getPositionCount(); pos++) {
            consumer.accept(pos);
        }
    }

    private <V extends Block, U> void assertByteRefsBlockContentEquals(Block input, Block result, BytesRef readBuffer) {
        assertBlockContentEquals(input, result, (BytesRefBlock b, Integer pos) -> b.getBytesRef(pos, readBuffer), BytesRefBlock.class);
    }

    protected EvalOperator.ExpressionEvaluator.Factory evaluatorFactory(int channel) {
        return context -> new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                return BlockUtils.deepCopyOf(page.getBlock(channel), context.blockFactory());
            }

            @Override
            public void close() {

            }
        };
    }
}
