/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.RerankAction;
import org.elasticsearch.xpack.esql.inference.InferenceService;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Computes relevance scores for rows with a reranking inference endpoint, sending <b>one</b> rerank request for all of
 * its input.
 * <p>
 * Listwise rerankers score each document relative to the other documents in the same request, so scores from separate
 * requests are not comparable and a later {@code SORT} on them would produce a wrong ranking. This operator buffers every
 * input page and, once its input is finished, sends every row in a single request, like the
 * {@code text_similarity_reranker} retriever does for its rank window. The input is bounded by the implicit limit that
 * {@code RERANK} adds below itself ({@code esql.command.rerank.limit}). {@link RerankOperator.Factory} uses this operator
 * for every endpoint that isn't known to be pointwise; pointwise endpoints keep the batched, concurrent
 * {@link RerankOperator}.
 * </p>
 * <p>
 * Each input page is emitted unchanged except for the score block, in input order. Rows are turned into documents and
 * scores are written back exactly as {@link RerankOperator} does. This operator must see every row, so it must run in a
 * single driver instance; {@code RERANK} runs on the coordinator after a global limit, which guarantees that.
 * </p>
 */
public class SingleRequestRerankOperator extends AsyncOperator<SingleRequestRerankOperator.Result> {

    private final InferenceService inferenceService;
    private final String inferenceId;
    private final String queryText;
    private final int scoreChannel;
    private final TimeValue timeout;
    private final ExpressionEvaluator[] inputEvaluators;
    private final RerankOutputBuilder outputBuilder;

    /**
     * Pages received but not yet sent. Only accessed on the driver thread.
     */
    private final List<Page> bufferedPages = new ArrayList<>();

    /**
     * Output pages built from the response but not yet returned by {@link #getOutput()}. Only accessed on the driver thread.
     */
    private final Deque<Page> outputPages = new ArrayDeque<>();

    private boolean requestStarted = false;

    /**
     * Pages received and pages covered by the request, for {@link #status}. Only accessed on the driver thread.
     */
    private long pagesReceived = 0;
    private long pagesSent = 0;

    /**
     * @param driverContext    The driver context.
     * @param inferenceService The inference service to use for executing the rerank request.
     * @param inferenceId      The ID of the reranking endpoint to invoke.
     * @param queryText        The query text to use for reranking.
     * @param inputEvaluators  Evaluators for the texts to rerank, one per {@code ON} field.
     * @param scoreChannel     The output channel where the relevance scores will be written.
     * @param timeout          Timeout for the rerank request, or {@code null} for the default.
     */
    SingleRequestRerankOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        String inferenceId,
        String queryText,
        ExpressionEvaluator[] inputEvaluators,
        int scoreChannel,
        @Nullable TimeValue timeout
    ) {
        super(driverContext, inferenceService.threadContext(), 1);
        this.inferenceService = inferenceService;
        this.inferenceId = inferenceId;
        this.queryText = queryText;
        this.scoreChannel = scoreChannel;
        this.timeout = timeout;
        this.inputEvaluators = inputEvaluators;
        this.outputBuilder = new RerankOutputBuilder(driverContext.blockFactory(), scoreChannel);
    }

    @Override
    public boolean needsInput() {
        return requestStarted == false;
    }

    @Override
    public void addInput(Page page) {
        assert requestStarted == false : "received input after the rerank request was started";
        bufferedPages.add(page);
        pagesReceived++;
    }

    @Override
    public void finish() {
        if (requestStarted == false) {
            requestStarted = true;
            if (bufferedPages.isEmpty() == false) {
                // Build the request before handing the pages over, so that if building fails they are still buffered
                // and released by doClose().
                Input input = buildInput(bufferedPages);
                List<Page> pages = List.copyOf(bufferedPages);
                bufferedPages.clear();
                pagesSent = pages.size();
                startAsync(() -> releasePagesOnAnyThread(pages), listener -> sendRequest(pages, input, listener));
            }
        }
        super.finish();
    }

    /**
     * Turns every row of every page into documents, in page order, and builds the request for all of them.
     */
    private Input buildInput(List<Page> pages) {
        List<String> documents = new ArrayList<>();
        List<int[]> positionValueCounts = new ArrayList<>(pages.size());
        BytesRef scratch = new BytesRef();
        for (Page page : pages) {
            BytesRefBlock[] inputBlocks = new BytesRefBlock[inputEvaluators.length];
            try {
                for (int i = 0; i < inputEvaluators.length; i++) {
                    inputBlocks[i] = (BytesRefBlock) inputEvaluators[i].eval(page);
                }
                int[] pageCounts = new int[page.getPositionCount()];
                for (int position = 0; position < page.getPositionCount(); position++) {
                    List<String> rowDocuments = RerankRequestIterator.readInputText(inputBlocks, position, scratch);
                    documents.addAll(rowDocuments);
                    pageCounts[position] = rowDocuments.size();
                }
                positionValueCounts.add(pageCounts);
            } finally {
                Releasables.closeExpectNoException(inputBlocks);
            }
        }
        return new Input(
            RerankRequestIterator.inferenceRequest(inferenceId, queryText, timeout, documents),
            documents.size(),
            positionValueCounts
        );
    }

    private void sendRequest(List<Page> pages, Input input, ActionListener<Result> listener) {
        // ActionListener.run fails the listener if dispatching throws, which releases the pages
        ActionListener.run(listener, l -> {
            if (input.request() == null) {
                l.onResponse(new Result(pages, input.positionValueCounts(), new double[0]));
                return;
            }
            inferenceService.executeRerankInference(
                input.request(),
                new ThreadedActionListener<>(
                    inferenceService.threadPool().executor(ThreadPool.Names.SEARCH),
                    l.delegateFailureAndWrap(
                        (delegate, response) -> delegate.onResponse(
                            new Result(pages, input.positionValueCounts(), scores(response, input.documentCount()))
                        )
                    )
                )
            );
        });
    }

    /**
     * Extracts the scores from {@code response} in document order, checking that there is one for every document that was
     * sent.
     */
    private double[] scores(InferenceAction.Response response, int documentCount) {
        double[] scores = RerankOutputBuilder.scores(response);
        if (scores.length != documentCount) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Inference endpoint [%s] returned scores for [%d] of the [%d] documents sent by RERANK. "
                        + "If the endpoint is configured with [top_n], set it to at least the number of documents RERANK receives, "
                        + "or reduce that number with LIMIT.",
                    inferenceId,
                    scores.length,
                    documentCount
                )
            );
        }
        return scores;
    }

    @Override
    public Page getOutput() {
        if (outputPages.isEmpty()) {
            Result result = fetchFromBuffer();
            if (result == null) {
                return null;
            }
            buildOutputPages(result);
        }
        return outputPages.poll();
    }

    /**
     * Builds one output page per input page with {@link RerankOutputBuilder}, the same way as {@link RerankOperator}: each
     * page gets the scores of its rows' documents. The input pages' blocks move into the output pages; if building fails,
     * every input page not yet turned into an output page is released.
     */
    private void buildOutputPages(Result result) {
        List<Page> inputPages = result.inputPages();
        int nextPage = 0;
        try {
            int firstScore = 0;
            for (; nextPage < inputPages.size(); nextPage++) {
                int[] pageCounts = result.positionValueCounts().get(nextPage);
                outputPages.add(outputBuilder.buildOutputPage(inputPages.get(nextPage), result.scores(), firstScore, pageCounts));
                firstScore += IntStream.of(pageCounts).sum();
            }
        } catch (Exception e) {
            releasePagesOnAnyThread(inputPages.subList(nextPage, inputPages.size()));
            throw e;
        }
    }

    @Override
    public boolean isFinished() {
        return super.isFinished() && outputPages.isEmpty();
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputPages.isEmpty() == false || super.canProduceMoreDataWithoutExtraInput();
    }

    /**
     * Reports pages rather than async actions, since this operator covers every page with a single action: every page
     * received so far, and every page covered by the request once it completes.
     */
    @Override
    protected Operator.Status status(long receivedActions, long completedActions, long processNanos) {
        return super.status(pagesReceived, completedActions > 0 ? pagesSent : 0, processNanos);
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<Result> listener) {
        throw new UnsupportedOperationException("SingleRequestRerankOperator buffers its input and sends a single request from finish()");
    }

    @Override
    protected void releaseFetchedOnAnyThread(Result result) {
        Releasables.closeExpectNoException(result);
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(
            () -> releasePagesOnAnyThread(bufferedPages),
            () -> releasePagesOnAnyThread(outputPages),
            () -> Releasables.closeExpectNoException(inputEvaluators)
        );
    }

    private static void releasePagesOnAnyThread(Iterable<Page> pages) {
        for (Page page : pages) {
            releasePageOnAnyThread(page);
        }
    }

    @Override
    public String toString() {
        return "RerankOperator[inference_id=[" + inferenceId + "], query=[" + queryText + "], score_channel=[" + scoreChannel + "]]";
    }

    /**
     * The request for all buffered pages.
     *
     * @param request             The rerank request, or {@code null} when no row contributed a document.
     * @param documentCount       The number of documents in the request.
     * @param positionValueCounts For each page, the number of documents each of its rows contributed.
     */
    private record Input(@Nullable RerankAction.Request request, int documentCount, List<int[]> positionValueCounts) {}

    /**
     * The response to the rerank request, with the pages it covers.
     *
     * @param inputPages          The buffered input pages, in input order.
     * @param positionValueCounts For each page, the number of documents each of its rows contributed.
     * @param scores              The score of each document, in document order. Empty when no row contributed a document.
     */
    record Result(List<Page> inputPages, List<int[]> positionValueCounts, double[] scores) implements Releasable {
        @Override
        public void close() {
            releasePagesOnAnyThread(inputPages);
        }
    }
}
