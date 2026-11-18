/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.engine;

import org.apache.lucene.index.MergePolicy;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.matview.MatviewPlugin;
import org.elasticsearch.xpack.matview.fold.FoldSpec;
import org.elasticsearch.xpack.matview.fold.FoldStrategy;
import org.elasticsearch.xpack.matview.fold.FoldingMergePolicy;
import org.elasticsearch.xpack.matview.fold.HashFoldStrategy;
import org.elasticsearch.xpack.matview.fold.RetentionFoldGuard;
import org.elasticsearch.xpack.matview.fold.SortedFoldStrategy;
import org.elasticsearch.xpack.matview.fold.compute.ComputeReducer;

import java.util.Locale;

/**
 * An {@link InternalEngine} whose merges fold rows sharing the view's group key. The folding policy is installed
 * from {@link #wrapMergePolicy}, outside the engine's retention and pruning policies: it then sees readers with
 * retention already applied, and nothing between it and the writer reads a wrapped reader's live docs before every
 * reader of the merge is wrapped, which the sorted strategy relies on. The strategy follows the index sort unless
 * the index settings pin one.
 */
public class FoldingEngine extends InternalEngine {

    private static final Logger logger = LogManager.getLogger(FoldingEngine.class);
    private static final int REDUCER_PAGE_SIZE = 4096;

    public FoldingEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected MergePolicy wrapMergePolicy(MergePolicy mergePolicy) {
        EngineConfig engineConfig = config();
        IndexSettings indexSettings = engineConfig.getIndexSettings();
        FoldSpec spec = MatviewPlugin.foldSpec(indexSettings);
        if (spec == null) {
            return super.wrapMergePolicy(mergePolicy);
        }
        FoldStrategy strategy = strategy(spec, indexSettings, engineConfig);
        var guard = new RetentionFoldGuard(this::getMinRetainedSeqNo, indexSettings.seqNoIndexOptions(), Lucene.SOFT_DELETES_FIELD);
        logger.info("folding [{}] with the [{}] strategy", indexSettings.getIndex().getName(), strategy.getClass().getSimpleName());
        return super.wrapMergePolicy(new FoldingMergePolicy(mergePolicy, spec, strategy, guard));
    }

    private static FoldStrategy strategy(FoldSpec spec, IndexSettings indexSettings, EngineConfig engineConfig) {
        String requested = indexSettings.getValue(MatviewPlugin.FOLD_STRATEGY).toLowerCase(Locale.ROOT);
        SortedFoldStrategy sorted = SortedFoldStrategy.forSort(spec, engineConfig.getIndexSort());
        return switch (requested) {
            case "auto" -> sorted != null ? sorted : hash(spec, engineConfig);
            case "sorted" -> {
                if (sorted == null) {
                    throw new IllegalArgumentException(
                        "[" + MatviewPlugin.FOLD_STRATEGY.getKey() + "] is [sorted] but the index sort does not start with the fold key"
                    );
                }
                yield sorted;
            }
            case "hash" -> hash(spec, engineConfig);
            default -> throw new IllegalArgumentException(
                "[" + MatviewPlugin.FOLD_STRATEGY.getKey() + "] must be auto, sorted or hash, got [" + requested + "]"
            );
        };
    }

    /** Reduction through the compute engine, with its memory accounted on the request breaker. */
    private static FoldStrategy hash(FoldSpec spec, EngineConfig engineConfig) {
        CircuitBreakerService breakers = engineConfig.getCircuitBreakerService();
        BigArrays bigArrays = new BigArrays(null, breakers, CircuitBreaker.REQUEST);
        BlockFactory blockFactory = BlockFactory.builder(bigArrays).breaker(breakers.getBreaker(CircuitBreaker.REQUEST)).build();
        return new HashFoldStrategy(new ComputeReducer(blockFactory, spec, REDUCER_PAGE_SIZE));
    }
}
