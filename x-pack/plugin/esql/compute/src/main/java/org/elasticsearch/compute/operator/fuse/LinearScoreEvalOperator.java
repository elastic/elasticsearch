/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.DoubleVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Updates the score column in two stages:
 * 1. Normalizes the scores using the normalization method specified in the config. Each row belongs
 * to a result group that is specified by the discriminator column. Scores are normalized for each result group.
 * 2. Multiplies the normalized score by the weight specified in the config. The config contains the weights that
 * we need to apply for each result group.
 *
 */
public class LinearScoreEvalOperator implements Operator {
    public record Factory(int discriminatorPosition, int scorePosition, LinearConfig linearConfig) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new LinearScoreEvalOperator(discriminatorPosition, scorePosition, linearConfig);
        }

        @Override
        public String describe() {
            return "LinearScoreEvalOperator[discriminatorPosition="
                + discriminatorPosition
                + ", scorePosition="
                + scorePosition
                + ", config="
                + linearConfig
                + "]";
        }
    }

    private final int scorePosition;
    private final int discriminatorPosition;
    private final LinearConfig config;
    private final Normalizer normalizer;

    private final Deque<Page> inputPages;
    private final Deque<Page> outputPages;
    private boolean finished;

    private long emitNanos;
    private int pagesReceived = 0;
    private int pagesProcessed = 0;
    private long rowsReceived = 0;
    private long rowsEmitted = 0;

    public LinearScoreEvalOperator(int discriminatorPosition, int scorePosition, LinearConfig config) {
        this.scorePosition = scorePosition;
        this.discriminatorPosition = discriminatorPosition;
        this.config = config;
        this.normalizer = createNormalizer(config.normalizer());

        finished = false;
        inputPages = new ArrayDeque<>();
        outputPages = new ArrayDeque<>();
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        inputPages.add(page);
        pagesReceived++;
        rowsReceived += page.getPositionCount();
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            createOutputPages();
        }
    }

    private void createOutputPages() {
        final var emitStart = System.nanoTime();
        normalizer.preprocess(inputPages, scorePosition, discriminatorPosition);
        try {
            while (inputPages.isEmpty() == false) {
                Page inputPage = inputPages.peek();
                processInputPage(inputPage);
                inputPages.removeFirst();
                pagesProcessed += 1;
            }
        } finally {
            emitNanos = System.nanoTime() - emitStart;
            Releasables.close(inputPages);
        }
    }

    private void processInputPage(Page inputPage) {
        BytesRefBlock discriminatorBlock = inputPage.getBlock(discriminatorPosition);
        DoubleVectorBlock initialScoreBlock = inputPage.getBlock(scorePosition);

        Page newPage = null;
        Block scoreBlock = null;
        DoubleVector.Builder scores = null;

        try {
            scores = discriminatorBlock.blockFactory().newDoubleVectorBuilder(discriminatorBlock.getPositionCount());

            for (int i = 0; i < inputPage.getPositionCount(); i++) {
                String discriminator = discriminatorBlock.getBytesRef(i, new BytesRef()).utf8ToString();

                var weight = config.weights().get(discriminator) == null ? 1.0 : config.weights().get(discriminator);

                double score = initialScoreBlock.getDouble(i);
                scores.appendDouble(weight * normalizer.normalize(score, discriminator));
            }

            scoreBlock = scores.build().asBlock();
            newPage = inputPage.appendBlock(scoreBlock);

            int[] projections = new int[newPage.getBlockCount() - 1];

            for (int i = 0; i < newPage.getBlockCount() - 1; i++) {
                projections[i] = i == scorePosition ? newPage.getBlockCount() - 1 : i;
            }

            outputPages.add(newPage.projectBlocks(projections));
        } finally {
            if (newPage != null) {
                newPage.releaseBlocks();
            }
            if (scoreBlock == null && scores != null) {
                Releasables.close(scores);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && outputPages.isEmpty();
    }

    @Override
    public Page getOutput() {
        if (finished == false || outputPages.isEmpty()) {
            return null;
        }

        Page page = outputPages.removeFirst();
        rowsEmitted += page.getPositionCount();

        return page;
    }

    @Override
    public void close() {
        for (Page page : inputPages) {
            page.releaseBlocks();
        }
        for (Page page : outputPages) {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "LinearScoreEvalOperator[discriminatorPosition="
            + discriminatorPosition
            + ", scorePosition="
            + scorePosition
            + ", config="
            + config
            + "]";
    }

    @Override
    public Operator.Status status() {
        return new Status(emitNanos, pagesReceived, pagesProcessed, rowsReceived, rowsEmitted);
    }

    public record Status(long emitNanos, int pagesReceived, int pagesProcessed, long rowsReceived, long rowsEmitted)
        implements
            Operator.Status {

        public static final TransportVersion ESQL_FUSE_LINEAR_OPERATOR_STATUS = TransportVersion.fromName(
            "esql_fuse_linear_operator_status"
        );

        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "linearScoreEval",
            Status::new
        );

        Status(StreamInput streamInput) throws IOException {
            this(streamInput.readLong(), streamInput.readInt(), streamInput.readInt(), streamInput.readLong(), streamInput.readLong());
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ESQL_FUSE_LINEAR_OPERATOR_STATUS);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            assert false : "must not be called when overriding supportsVersion";
            throw new UnsupportedOperationException("must not be called when overriding supportsVersion");
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

    private Normalizer createNormalizer(LinearConfig.Normalizer normalizer) {
        return switch (normalizer) {
            case NONE -> new NoneNormalizer();
            case L2_NORM -> new L2NormNormalizer();
            case MINMAX -> new MinMaxNormalizer();
        };
    }

    private interface Normalizer {
        double normalize(double score, String discriminator);

        void preprocess(Collection<Page> inputPages, int scorePosition, int discriminatorPosition);
    }

    private class NoneNormalizer implements Normalizer {
        @Override
        public double normalize(double score, String discriminator) {
            return score;
        }

        @Override
        public void preprocess(Collection<Page> inputPages, int scorePosition, int discriminatorPosition) {}
    }

    private class L2NormNormalizer implements Normalizer {
        private final Map<String, Double> l2Norms = new HashMap<>();

        @Override
        public double normalize(double score, String discriminator) {
            var l2Norm = l2Norms.get(discriminator);
            assert l2Norm != null;
            return l2Norms.get(discriminator) == 0.0 ? 0.0 : score / l2Norm;
        }

        @Override
        public void preprocess(Collection<Page> inputPages, int scorePosition, int discriminatorPosition) {
            for (Page inputPage : inputPages) {
                DoubleVectorBlock scoreBlock = inputPage.getBlock(scorePosition);
                BytesRefBlock discriminatorBlock = inputPage.getBlock(discriminatorPosition);

                for (int i = 0; i < inputPage.getPositionCount(); i++) {
                    double score = scoreBlock.getDouble(i);
                    String discriminator = discriminatorBlock.getBytesRef(i, new BytesRef()).utf8ToString();

                    l2Norms.compute(discriminator, (k, v) -> v == null ? score * score : v + score * score);
                }
            }

            l2Norms.replaceAll((k, v) -> Math.sqrt(v));
        }
    }

    private class MinMaxNormalizer implements Normalizer {
        private final Map<String, Double> minScores = new HashMap<>();
        private final Map<String, Double> maxScores = new HashMap<>();

        @Override
        public double normalize(double score, String discriminator) {
            var min = minScores.get(discriminator);
            var max = maxScores.get(discriminator);

            assert min != null;
            assert max != null;

            if (min.equals(max)) {
                return 0.0;
            }

            return (score - min) / (max - min);
        }

        @Override
        public void preprocess(Collection<Page> inputPages, int scorePosition, int discriminatorPosition) {
            for (Page inputPage : inputPages) {
                DoubleVectorBlock scoreBlock = inputPage.getBlock(scorePosition);
                BytesRefBlock discriminatorBlock = inputPage.getBlock(discriminatorPosition);

                for (int i = 0; i < inputPage.getPositionCount(); i++) {
                    double score = scoreBlock.getDouble(i);
                    String discriminator = discriminatorBlock.getBytesRef(i, new BytesRef()).utf8ToString();

                    minScores.compute(discriminator, (key, value) -> value == null ? score : Math.min(value, score));
                    maxScores.compute(discriminator, (key, value) -> value == null ? score : Math.max(value, score));
                }
            }
        }
    }
}
