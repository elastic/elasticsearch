/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
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
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class InferenceOperatorTestCase<InferenceResultsType extends InferenceServiceResults> extends OperatorTestCase {
    private ThreadPool threadPool;
    protected List<ElementType> elementTypes;
    protected int channelCount;

    @Before
    public void initChannels() {
        channelCount = between(2, 3);
        elementTypes = randomElementTypes(channelCount);
    }

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
        return new AbstractBlockSourceOperator(blockFactory, 8 * 1024) {
            @Override
            protected int remaining() {
                return size - currentPosition;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                currentPosition += length;
                Block[] blocks = new Block[channelCount];
                try {
                    for (int channel = 0; channel < channelCount; channel++) {
                        blocks[channel] = createInputBlock(blockFactory, elementTypes.get(channel), channel, length);
                    }
                    return new Page(blocks);
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw (e);
                }
            }
        };
    }

    private Block createInputBlock(BlockFactory blockFactory, ElementType elementType, int channel, int size) {
        return RandomBlock.randomBlock(blockFactory, elementType, size, false, channel == 0 ? false : randomBoolean(), 1, 2, 1, 3).block();
    }

    private List<ElementType> randomElementTypes(int channelCount) {
        return IntStream.range(0, channelCount).mapToObj(this::randomElementType).collect(Collectors.toList());
    }

    protected ElementType randomElementType(int channel) {
        return channel == 0 ? ElementType.BYTES_REF : randomValueOtherThan(ElementType.FLOAT, RandomBlock::randomElementType);
    }

    @Override
    public void testOperatorStatus() throws IOException {
        DriverContext driverContext = driverContext();
        try (var operator = simple().get(driverContext)) {
            AsyncOperator.Status status = asInstanceOf(AsyncOperator.Status.class, operator.status());

            assertThat(status, notNullValue());
            assertThat(status.receivedPages(), equalTo(0L));
            assertThat(status.completedPages(), equalTo(0L));
            assertThat(status.procesNanos(), greaterThanOrEqualTo(0L));
        }
    }

    protected InferenceRunner mockedSimpleInferenceRunner() {
        InferenceRunner inferenceRunner = mock(InferenceRunner.class);
        when(inferenceRunner.threadPool()).thenReturn(threadPool());
        doAnswer(i -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceAction.Response> listener = i.getArgument(1, ActionListener.class);
            InferenceAction.Request request = i.getArgument(0, InferenceAction.Request.class);
            InferenceAction.Response inferenceResponse = mock(InferenceAction.Response.class);
            when(inferenceResponse.getResults()).thenReturn(mockInferenceResult(request));
            listener.onResponse(inferenceResponse);
            return null;
        }).when(inferenceRunner).doInference(any(InferenceAction.Request.class), any());
        return inferenceRunner;
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
