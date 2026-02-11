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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AsyncOperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.AbstractBlockSourceOperator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class InferenceOperatorTestCase<InferenceResultsType extends InferenceServiceResults> extends AsyncOperatorTestCase {
    protected ThreadPool threadPool;
    protected int inputsCount;

    @Before
    public void setThreadPool() {
        threadPool = createThreadPool(
            new ScalingExecutorBuilder(
                "inference_response",
                0,
                10,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.inference.inference_response_thread_pool"
            )
        );
    }

    @Before
    public void initChannels() {
        inputsCount = randomIntBetween(1, 10);
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    protected ThreadPool threadPool() {
        return threadPool;
    }

    @Override
    protected int largeInputSize() {
        return between(100, 1_000);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new AbstractBlockSourceOperator(blockFactory, between(100, 8 * 1024)) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                length = Integer.min(length, remaining());
                Block[] blocks = new Block[inputsCount];
                try {
                    for (int b = 0; b < inputsCount; b++) {
                        try (var builder = blockFactory.newBytesRefBlockBuilder(length)) {
                            for (int i = 0; i < length; i++) {
                                if (randomInt() % 100 == 0) {
                                    builder.appendNull();
                                } else {
                                    builder.appendBytesRef(new BytesRef(randomAlphaOfLength(10)));
                                }
                            }
                            blocks[b] = builder.build();
                        }
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }

                currentPosition += length;
                return new Page(blocks);

            }
        };
    }

    @SuppressWarnings("unchecked")
    protected InferenceService mockedInferenceService() {
        return mockedInferenceService(new AtomicBoolean(false), new RuntimeException("default error"));
    }

    @SuppressWarnings("unchecked")
    protected InferenceService mockedInferenceService(AtomicBoolean shouldFail, Exception failureException) {
        Client mockClient = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                runWithRandomDelay(() -> {
                    if (action instanceof InferenceAction && request instanceof InferenceAction.Request inferenceRequest) {
                        if (shouldFail.get()) {
                            listener.onFailure(failureException);
                            return;
                        }
                        listener.onResponse((Response) new InferenceAction.Response(mockInferenceResult(inferenceRequest)));
                        return;
                    }

                    listener.onFailure(new UnsupportedOperationException("Unexpected action: " + action));
                });
            }

            private void runWithRandomDelay(Runnable runnable) {
                threadPool.schedule(runnable, TimeValue.timeValueNanos(between(1, 1_000)), threadPool.executor("inference_response"));
            }
        };

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet<>(InferenceSettings.getSettings()));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);

        return new InferenceService(mockClient, clusterService);
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
                Block b = page.getBlock(channel);
                b.incRef();
                return b;
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {

            }
        };
    }

    public static class BlockStringReader {

        private final StringBuilder sb = new StringBuilder();
        private BytesRef scratch = new BytesRef();

        public String readString(BytesRefBlock block, int pos) {
            sb.setLength(0);
            int valueIndex = block.getFirstValueIndex(pos);
            while (valueIndex < block.getFirstValueIndex(pos) + block.getValueCount(pos)) {
                scratch = block.getBytesRef(valueIndex, scratch);
                sb.append(scratch.utf8ToString());
                if (valueIndex < block.getValueCount(pos) - 1) {
                    sb.append("\n");
                }
                valueIndex++;
            }
            scratch = block.getBytesRef(block.getFirstValueIndex(pos), scratch);

            return sb.toString();
        }

    }
}
