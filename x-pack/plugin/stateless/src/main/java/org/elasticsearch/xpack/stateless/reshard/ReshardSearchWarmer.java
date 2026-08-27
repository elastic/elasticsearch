/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

/**
 * Starts background warming of current search readers when an index enters an active split.
 */
public final class ReshardSearchWarmer implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(ReshardSearchWarmer.class);

    private final Executor warmerExecutor;

    /**
     * Creates a listener that dispatches warming work to {@code warmerExecutor}.
     */
    public ReshardSearchWarmer(Executor warmerExecutor) {
        this.warmerExecutor = Objects.requireNonNull(warmerExecutor);
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        final var splitActive = new AtomicBoolean(hasActiveSplit(indexService.getMetadata()));
        indexService.addMetadataListener(indexMetadata -> {
            final boolean active = hasActiveSplit(indexMetadata);
            if (splitActive.getAndSet(active) == false && active) {
                scheduleCurrentReaders(indexService);
            }
        });
    }

    private void scheduleCurrentReaders(IndexService indexService) {
        try {
            warmerExecutor.execute(() -> {
                for (var indexShard : indexService) {
                    try {
                        indexShard.withEngine(engine -> {
                            if (engine instanceof SearchEngine searchEngine) {
                                searchEngine.maybeWarmCurrentReaderForResharding();
                            }
                            return null;
                        });
                    } catch (Exception e) {
                        logger.debug(
                            () -> format(
                                "failed to schedule resharding unowned-document bitset warming for shard [%s]",
                                indexShard.shardId()
                            ),
                            e
                        );
                    }
                }
            });
        } catch (RuntimeException e) {
            logger.debug(
                () -> format("failed to schedule resharding unowned-document bitset warming for index [%s]", indexService.index()),
                e
            );
        }
    }

    private static boolean hasActiveSplit(IndexMetadata indexMetadata) {
        return indexMetadata.getReshardingMetadata() != null && indexMetadata.getReshardingMetadata().isSplit();
    }
}
