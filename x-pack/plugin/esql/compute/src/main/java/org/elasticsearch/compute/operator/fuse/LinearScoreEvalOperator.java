/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.DoubleVectorBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;

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
    }

    @Override
    public void finish() {
        if (finished == false) {
            finished = true;
            createOutputPages();
        }
    }

    private void createOutputPages() {
        normalizer.preprocess(inputPages, scorePosition, discriminatorPosition);

        while (inputPages.isEmpty() == false) {
            Page inputPage = inputPages.peek();

            BytesRefBlock discriminatorBlock = inputPage.getBlock(discriminatorPosition);
            DoubleVectorBlock initialScoreBlock = inputPage.getBlock(scorePosition);

            DoubleVector.Builder scores = discriminatorBlock.blockFactory().newDoubleVectorBuilder(discriminatorBlock.getPositionCount());

            for (int i = 0; i < inputPage.getPositionCount(); i++) {
                String discriminator = discriminatorBlock.getBytesRef(i, new BytesRef()).utf8ToString();

                var weight = config.weights().get(discriminator) == null ? 1.0 : config.weights().get(discriminator);

                Double score = initialScoreBlock.getDouble(i);
                scores.appendDouble(weight * normalizer.normalize(score, discriminator));
            }
            Block scoreBlock = scores.build().asBlock();
            inputPage = inputPage.appendBlock(scoreBlock);

            int[] projections = new int[inputPage.getBlockCount() - 1];

            for (int i = 0; i < inputPage.getBlockCount() - 1; i++) {
                projections[i] = i == scorePosition ? inputPage.getBlockCount() - 1 : i;
            }
            inputPages.removeFirst();
            outputPages.add(inputPage.projectBlocks(projections));
            inputPage.releaseBlocks();
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
        return outputPages.removeFirst();
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
