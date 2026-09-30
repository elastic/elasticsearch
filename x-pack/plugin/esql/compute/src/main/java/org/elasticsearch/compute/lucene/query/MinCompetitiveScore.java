/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNOperator;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * Reads the {@code _score} of the least competitive row kept by the {@link TopNOperator}s of a
 * {@code SORT _score DESC | LIMIT N} query out of a {@link SharedMinCompetitive} so that
 * {@link LuceneSourceOperator} can hand it to Lucene via {@link Scorable#setMinCompetitiveScore}.
 * <p>
 * This is the score counterpart of {@link MinCompetitiveQuery}: a field-valued bound can be turned
 * into a range query, but a score bound can only be expressed through the scorer itself, which lets
 * WAND / block-max scorers skip whole blocks of documents that can't beat the current top N. That
 * requires the weight to be built with {@link ScoreMode#TOP_SCORES}.
 * <p>
 * Every operator gets its own instance; the {@link SharedMinCompetitive} behind it is shared by all
 * drivers running the same TopN on the node. Instances are not thread safe, but the driver only
 * ever calls them from one thread at a time.
 */
public final class MinCompetitiveScore implements Releasable {
    /**
     * Returned by {@link #minCompetitiveScore()} while no {@link TopNOperator} has a full heap yet.
     * Every Lucene score is {@code >= 0} so this never lets Lucene skip anything.
     */
    public static final float NO_THRESHOLD = 0f;

    /**
     * Builds a {@link MinCompetitiveScore} per {@link LuceneSourceOperator}.
     *
     * @param minCompetitive the channel the {@link TopNOperator}s publish into. It must track exactly
     *                       one {@link ElementType#DOUBLE} key sorted descending: skipping low scores
     *                       is only valid when high scores win.
     */
    public record Factory(SharedMinCompetitive.Supplier minCompetitive) {
        public Factory {
            List<SharedMinCompetitive.KeyConfig> configs = minCompetitive.keyConfigs();
            if (configs.size() != 1) {
                throw new IllegalArgumentException("min competitive score requires exactly one sort key but got " + configs);
            }
            SharedMinCompetitive.KeyConfig config = configs.getFirst();
            if (config.elementType() != ElementType.DOUBLE || config.asc()) {
                throw new IllegalArgumentException("min competitive score requires a descending DOUBLE sort key but got " + config);
            }
        }

        public MinCompetitiveScore build(BlockFactory blockFactory) {
            return new MinCompetitiveScore(blockFactory, minCompetitive.get());
        }
    }

    private final BlockFactory blockFactory;
    private final SharedMinCompetitive minCompetitive;

    /**
     * The {@link SharedMinCompetitive#generation()} that {@link #cachedScore} was decoded from.
     * Starts at {@code -1} so the first read always decodes.
     */
    private long cachedGeneration = -1;
    private float cachedScore = NO_THRESHOLD;

    /**
     * How many times we decoded a new value from {@link #minCompetitive}. Visible for testing.
     */
    private int decodes;

    private MinCompetitiveScore(BlockFactory blockFactory, SharedMinCompetitive minCompetitive) {
        this.blockFactory = blockFactory;
        this.minCompetitive = minCompetitive;
    }

    /**
     * The current minimum competitive score, or {@link #NO_THRESHOLD} if there isn't one yet. It is
     * safe to skip every document scoring strictly less than this. Successive calls never return a
     * smaller value, which {@link Scorable#setMinCompetitiveScore} requires.
     * <p>
     * Cheap when nothing changed: it takes the channel's lock once to read its generation and
     * decodes the shared bound only after a {@link TopNOperator} published a more competitive one.
     */
    public float minCompetitiveScore() {
        long generation = minCompetitive.generation();
        if (generation != cachedGeneration) {
            /*
             * The bound may advance between reading the generation and reading the value. Then we
             * cache a value that's newer than its generation and decode again on the next call.
             * That's fine: newer values are only ever more competitive, and still sound.
             */
            float decoded = decode();
            // Paranoia: SharedMinCompetitive only accepts more competitive values, but Lucene requires monotonicity.
            cachedScore = Math.max(cachedScore, decoded);
            cachedGeneration = generation;
            decodes++;
        }
        return cachedScore;
    }

    private float decode() {
        Page page = minCompetitive.get(blockFactory);
        if (page == null) {
            return NO_THRESHOLD;
        }
        try {
            DoubleBlock block = page.getBlock(0);
            if (block.isNull(0)) {
                // The least competitive kept row has no score. _score is never null so this shouldn't happen.
                return NO_THRESHOLD;
            }
            return toMinCompetitiveFloat(block.getDouble(block.getFirstValueIndex(0)));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Convert the {@code double} ES|QL {@code _score} into the {@code float} Lucene compares scores
     * against, rounding <strong>down</strong> so we never skip a document whose score equals the
     * bound. ES|QL builds {@code _score} by widening Lucene's {@code float} score so the conversion
     * is exact in practice. Rounding down only matters if something ever feeds a non-float value.
     */
    static float toMinCompetitiveFloat(double score) {
        if (Double.isNaN(score) || score <= 0) {
            return NO_THRESHOLD;
        }
        float f = (float) score;
        if (f > score) {
            f = Math.nextDown(f);
        }
        return f;
    }

    int decodes() {
        return decodes;
    }

    @Override
    public void close() {
        minCompetitive.decRef();
    }
}
