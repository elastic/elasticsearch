/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
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
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.AbstractBlockSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RerankOperatorTests extends OperatorTestCase {
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";
    private static final String SIMPLE_INFERENCE_ID = "test_reranker";
    private static final String SIMPLE_QUERY = "query text";
    private ThreadPool threadPool;
    private List<ElementType> inputChannelElementTypes;
    private XContentRowEncoder.Factory rowEncoderFactory;
    private int scoreChannel;
    private int encodedRowLength;

    @Before
    private void initChannels() {
        int channelCount = between(2, 10);
        scoreChannel = between(0, channelCount - 1);
        inputChannelElementTypes = IntStream.range(0, channelCount).sorted().mapToObj(this::randomElementType).collect(Collectors.toList());
        rowEncoderFactory = mockRowEncoderFactory();
        encodedRowLength = between(1, 10);
    }

    @Before
    public void setThreadPool() {
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        InferenceRunner inferenceRunner = mockedSimpleInferenceRunner();
        return new RerankOperator.Factory(inferenceRunner, SIMPLE_INFERENCE_ID, SIMPLE_QUERY, rowEncoderFactory, scoreChannel);
    }

    private InferenceRunner mockedSimpleInferenceRunner() {
        return new InferenceRunner(mockedClient());
    }

    private Client mockedClient() {
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        doAnswer(i -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceAction.Response> listener = i.getArgument(2, ActionListener.class);
            listener.onResponse(mockedInferenceResponse(i.getArgument(1, InferenceAction.Request.class)));
            return null;
        }).when(client).execute(eq(InferenceAction.INSTANCE), any(InferenceAction.Request.class), any());
        return client;
    }

    private InferenceAction.Response mockedInferenceResponse(InferenceAction.Request request) throws Exception {
        InferenceAction.Response inferenceResponse = mock(InferenceAction.Response.class);
        List<RankedDocsResults.RankedDoc> rankedDocs = new ArrayList<>();
        for (int rank = 0; rank < request.getInput().size(); rank++) {
            String text = request.getInput().get(rank);
            rankedDocs.add(new RankedDocsResults.RankedDoc(rank, 1f / text.length(), text));
        }

        when(inferenceResponse.getResults()).thenReturn(new RankedDocsResults(rankedDocs));

        return inferenceResponse;
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return expectedToStringOfSimple();
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo(
            "RerankOperator[inference_id=[" + SIMPLE_INFERENCE_ID + "], query=[" + SIMPLE_QUERY + "], score_channel=[" + scoreChannel + "]]"
        );
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
                Block[] blocks = new Block[inputChannelElementTypes.size()];
                try {
                    currentPosition += length;
                    for (int b = 0; b < inputChannelElementTypes.size(); b++) {
                        blocks[b] = RandomBlock.randomBlock(
                            blockFactory,
                            inputChannelElementTypes.get(b),
                            length,
                            randomBoolean(),
                            0,
                            10,
                            0,
                            10
                        ).block();
                        blocks[b].allowPassingToDifferentDriver();
                    }
                    return new Page(blocks);
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw (e);
                }
            }
        };
    }

    /**
     * Ensures that the Operator.Status of this operator has the standard fields.
     */
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

    @Override
    protected void assertSimpleOutput(List<Page> inputPages, List<Page> resultPages) {
        assertThat(inputPages, hasSize(resultPages.size()));

        for (int pageId = 0; pageId < inputPages.size(); pageId++) {
            Page inputPage = inputPages.get(pageId);
            Page resultPage = resultPages.get(pageId);

            // Check all rows are present and the output shape is unchanged.
            assertThat(inputPage.getPositionCount(), equalTo(resultPage.getPositionCount()));
            assertThat(inputPage.getBlockCount(), equalTo(resultPage.getBlockCount()));

            BytesRef readBuffer = new BytesRef();

            for (int channel = 0; channel < inputPage.getBlockCount(); channel++) {
                Block inputBlock = inputPage.getBlock(channel);
                Block resultBlock = resultPage.getBlock(channel);

                assertThat(resultBlock.getPositionCount(), equalTo(resultPage.getPositionCount()));
                assertThat(resultBlock.elementType(), equalTo(inputBlock.elementType()));

                if (channel == scoreChannel) {
                    assertExpectedScore(asInstanceOf(DoubleBlock.class, resultBlock));
                } else {
                    switch (inputBlock.elementType()) {
                        case BOOLEAN -> assertBlockContentEquals(inputBlock, resultBlock, BooleanBlock::getBoolean, BooleanBlock.class);
                        case INT -> assertBlockContentEquals(inputBlock, resultBlock, IntBlock::getInt, IntBlock.class);
                        case LONG -> assertBlockContentEquals(inputBlock, resultBlock, LongBlock::getLong, LongBlock.class);
                        case FLOAT -> assertBlockContentEquals(inputBlock, resultBlock, FloatBlock::getFloat, FloatBlock.class);
                        case DOUBLE -> assertBlockContentEquals(inputBlock, resultBlock, DoubleBlock::getDouble, DoubleBlock.class);
                        case BYTES_REF -> assertByteRefsBlockContentEquals(inputBlock, resultBlock, readBuffer);
                        default -> throw new AssertionError(
                            LoggerMessageFormat.format("Unexpected block type {}", inputBlock.elementType())
                        );
                    }
                }
            }
        }
    }

    private int inputChannelCount() {
        return inputChannelElementTypes.size();
    }

    private ElementType randomElementType(int channel) {
        return channel == scoreChannel ? ElementType.DOUBLE : RandomBlock.randomElementType();
    }

    private XContentRowEncoder.Factory mockRowEncoderFactory() {
        XContentRowEncoder.Factory factory = mock(XContentRowEncoder.Factory.class);
        doAnswer(factoryInvocation -> {
            DriverContext driverContext = factoryInvocation.getArgument(0, DriverContext.class);
            XContentRowEncoder rowEncoder = mock(XContentRowEncoder.class);
            doAnswer(encoderInvocation -> {
                Page inputPage = encoderInvocation.getArgument(0, Page.class);
                BytesRef scratch = new BytesRef(randomRealisticUnicodeOfCodepointLength(encodedRowLength));
                return driverContext.blockFactory().newConstantBytesRefBlockWith(scratch, inputPage.getPositionCount());
            }).when(rowEncoder).eval(any(Page.class));

            return rowEncoder;
        }).when(factory).get(any(DriverContext.class));

        return factory;
    }

    private void assertExpectedScore(DoubleBlock scoreBlockResult) {
        assertAllPositions(scoreBlockResult, (pos) -> {
            assertThat(scoreBlockResult.getValueCount(pos), equalTo(1));
            assertThat(scoreBlockResult.getDouble(scoreBlockResult.getFirstValueIndex(pos)), equalTo((double) (1f / encodedRowLength)));
        });
    }

    <V extends Block, U> void assertBlockContentEquals(
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
}
