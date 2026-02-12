/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversification;
import org.elasticsearch.search.diversification.mmr.MMRResultDiversificationContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ES|QL Operator for performing MMR result diversification
 */
public class MMROperator extends CompleteInputCollectorOperator {

    /**
     * Factor creation for the MMR Operator
     * @param diversificationField the name of the diversification field
     * @param diversificationFieldChannel the channel of the diversification field
     * @param limit the total number of results to emit
     * @param queryVector the (optional) query vector data for comparison
     * @param lambda the (optional) lambda value for the MMR diversification
     */
    public record Factory(
        String diversificationField,
        int diversificationFieldChannel,
        int limit,
        @Nullable VectorData queryVector,
        @Nullable Float lambda
    ) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new MMROperator(diversificationField, diversificationFieldChannel, limit, queryVector, lambda);
        }

        @Override
        public String describe() {
            return "MMROperator[diversificationField="
                + diversificationField
                + " (channel="
                + diversificationFieldChannel
                + "), limit="
                + limit
                + ", queryVector="
                + (queryVector != null ? queryVector.toString() : "null")
                + ", lambda="
                + (lambda != null ? lambda.toString() : "null")
                + "]";
        }
    }

    public static Float DEFAULT_LAMBDA = 0.5f;

    private final String diversifyField;
    private final int diversifyFieldChannel;
    private final int limit;
    private final VectorData queryVector;
    private final Float lambda;

    private final Deque<Page> outputPages = new ArrayDeque<>();
    private boolean outputPagesCreated = false;

    private long emitNanos;
    private int pagesProcessed = 0;
    private long rowsEmitted = 0L;

    MMROperator(String diversifyField, int diversifyFieldChannel, int limit, @Nullable VectorData queryVector, @Nullable Float lambda) {
        super();
        this.diversifyField = diversifyField;
        this.diversifyFieldChannel = diversifyFieldChannel;
        this.limit = limit;
        this.queryVector = queryVector;
        this.lambda = lambda;
    }

    @Override
    protected void onFinished() {
        // no additional implementation needed
    }

    @Override
    protected boolean isOperatorFinished() {
        return outputPagesCreated && outputPages.isEmpty();
    }

    @Override
    protected Page onGetOutput() {
        final var emitStart = System.nanoTime();

        // now we can perform our work
        // gather our input rows
        try {
            if (outputPagesCreated == false) {
                createOutputPages();
            }

            if (outputPages.isEmpty() == false) {
                Page page = outputPages.removeFirst();
                rowsEmitted += page.getPositionCount();
                return page;
            }
        } finally {
            emitNanos = System.nanoTime() - emitStart;
        }
        return null;
    }

    public record PagePositionDocVector(int page, int position, RankDoc doc, VectorData vector) {}

    private void createOutputPages() {
        // gather our documents and their vectors
        List<PagePositionDocVector> docsAndVectors = gatherAllDocsAndVectors();

        // set our doc ranks and final mappings for diversification
        docsAndVectors.sort(Comparator.comparing(PagePositionDocVector::doc));

        // keep this mapping so we know where the docs came from
        Map<Integer, Tuple<Integer, Integer>> mapRankToPageAndPosition = new HashMap<>();
        List<RankDoc> docs = new ArrayList<>();
        Map<Integer, VectorData> vectors = new HashMap<>();

        int docRank = 1;
        for (PagePositionDocVector docValues : docsAndVectors) {
            docValues.doc.rank = docRank;
            docs.add(docValues.doc);
            vectors.put(docRank, docValues.vector);
            mapRankToPageAndPosition.put(docValues.doc.rank, new Tuple<>(docValues.page, docValues.position));
            docRank++;
        }

        var diversificationContext = new MMRResultDiversificationContext(
            diversifyField,
            lambda == null ? DEFAULT_LAMBDA : lambda,
            limit,
            () -> queryVector
        );
        diversificationContext.setFieldVectors(vectors);

        var diversification = new MMRResultDiversification(diversificationContext);
        RankDoc[] results = null;
        try {
            results = diversification.diversify(docs.toArray(new RankDoc[0]));
        } catch (IOException ioEx) {
            throw new UncheckedIOException(ioEx);
        }

        // create our output filter set
        Map<Integer, List<Integer>> filtersByPagePosition = createOutputPageFilters(results, mapRankToPageAndPosition);
        createOutputPagesFromResults(filtersByPagePosition);

        outputPagesCreated = true;
    }

    private List<PagePositionDocVector> gatherAllDocsAndVectors() {
        List<PagePositionDocVector> docsAndVectors = new ArrayList<>();
        int pageIndex = 0;
        for (Page page : inputPages) {
            Block diversificationFieldBlock = page.getBlock(diversifyFieldChannel);
            var interleaved = new RankDocAndVector((FloatBlock) diversificationFieldBlock);
            int positionIndex = 0;
            for (Tuple<RankDoc, VectorData> docValues : interleaved) {
                docsAndVectors.add(new PagePositionDocVector(pageIndex, positionIndex, docValues.v1(), docValues.v2()));
                positionIndex++;
            }
            pageIndex++;
            pagesProcessed++;
        }
        return docsAndVectors;
    }

    private void createOutputPagesFromResults(Map<Integer, List<Integer>> filtersByPagePosition) {
        // output block types should be the same as the input
        // create our output pages filtered, in page order
        int pageCounter = 0;
        for (Page inputPage : inputPages) {
            var pagePositionsToKeep = filtersByPagePosition.getOrDefault(pageCounter, null);
            if (pagePositionsToKeep == null || pagePositionsToKeep.isEmpty()) {
                continue;
            }

            var pageFilter = pagePositionsToKeep.stream().mapToInt(i -> i).toArray();
            var outputBlocks = new Block[inputPage.getBlockCount()];
            boolean wasAdded = false;
            try {
                for (int b = 0; b < outputBlocks.length; b++) {
                    outputBlocks[b] = inputPage.getBlock(b).filter(false, pageFilter);
                }
                outputPages.addLast(new Page(outputBlocks));
                wasAdded = true;
            } finally {
                if (wasAdded == false) {
                    Releasables.closeExpectNoException(inputPage::releaseBlocks, Releasables.wrap(outputBlocks));
                }
            }
        }
    }

    private Map<Integer, List<Integer>> createOutputPageFilters(
        RankDoc[] results,
        Map<Integer, Tuple<Integer, Integer>> mapRankToPageAndPosition
    ) {
        Map<Integer, List<Integer>> filtersByPagePosition = new HashMap<>();
        for (int i = 0; i < results.length; i++) {
            int rank = results[i].rank;
            var pageAndRow = mapRankToPageAndPosition.get(rank);
            var filter = filtersByPagePosition.getOrDefault(pageAndRow.v1(), new ArrayList<>());
            filter.add(pageAndRow.v2());
            filtersByPagePosition.put(pageAndRow.v1(), filter);
        }
        return filtersByPagePosition;
    }

    @Override
    protected void onClose() {
        for (Page outputPage : outputPages) {
            outputPage.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "MMROperator[diversificationField="
            + diversifyField
            + " (channel="
            + diversifyFieldChannel
            + "), limit="
            + limit
            + ", queryVector="
            + (queryVector != null ? queryVector.toString() : "null")
            + ", lambda="
            + (lambda != null ? lambda.toString() : "null")
            + "]";
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return false;
    }

    @Override
    public Operator.Status status() {
        return new MMROperator.Status(emitNanos, pagesReceived, pagesProcessed, rowsReceived, rowsEmitted);
    }

    public record Status(long emitNanos, int pagesReceived, int pagesProcessed, long rowsReceived, long rowsEmitted)
        implements
            Operator.Status {

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "MMR",
            MMROperator.Status::new
        );

        Status(StreamInput streamInput) throws IOException {
            this(streamInput.readLong(), streamInput.readInt(), streamInput.readInt(), streamInput.readLong(), streamInput.readLong());
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(emitNanos);
            out.writeInt(pagesReceived);
            out.writeInt(pagesProcessed);
            out.writeLong(rowsReceived);
            out.writeLong(rowsEmitted);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("emit_nanos", emitNanos);
            if (builder.humanReadable()) {
                builder.field("emit_time", TimeValue.timeValueNanos(emitNanos));
            }
            builder.field("pages_received", pagesReceived);
            builder.field("pages_processed", pagesProcessed);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            return builder.endObject();
        }
    }

    public static class RankDocAndVector implements Iterable<Tuple<RankDoc, VectorData>> {

        private final FloatBlock vectorBlock;
        private final int numPositions;

        public RankDocAndVector(FloatBlock vectorBlock) {
            this.vectorBlock = vectorBlock;
            this.numPositions = vectorBlock.getPositionCount();
        }

        @Override
        public Iterator<Tuple<RankDoc, VectorData>> iterator() {
            return new RankDocAndVectorIterator(numPositions, vectorBlock);
        }
    }

    public static class RankDocAndVectorIterator implements Iterator<Tuple<RankDoc, VectorData>> {

        private final FloatBlock vectorBlock;
        private final int numPositions;
        private int currentPosition = 0;

        public RankDocAndVectorIterator(int numPositions, FloatBlock vectorBlock) {
            this.vectorBlock = vectorBlock;
            this.numPositions = numPositions;
        }

        @Override
        public boolean hasNext() {
            return currentPosition < numPositions;
        }

        @Override
        public Tuple<RankDoc, VectorData> next() {
            if (currentPosition >= numPositions) {
                return null;
            }

            var valueIndex = vectorBlock.getFirstValueIndex(currentPosition);
            var valueCount = vectorBlock.getValueCount(currentPosition);
            float[] vectorValues = new float[valueCount];
            for (int position = 0; position < valueCount; position++) {
                vectorValues[position] = vectorBlock.getFloat(valueIndex + position);
            }

            currentPosition++;

            // create pseudo-doc to pass into MMR diversifier
            return new Tuple<>(new RankDoc(currentPosition, 1.0f, 0), new VectorData(vectorValues));
        }
    }
}
