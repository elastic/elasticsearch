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
import org.elasticsearch.xpack.stateless.engine.SearchEngine;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts background warming of current search readers when an index enters an active split.
 */
public final class ReshardSearchWarmer implements IndexEventListener {

    @Override
    public void afterIndexCreated(IndexService indexService) {
        final var splitActive = new AtomicBoolean(hasActiveSplit(indexService.getMetadata()));
        indexService.addMetadataListener(indexMetadata -> {
            final var active = hasActiveSplit(indexMetadata);
            if (splitActive.getAndSet(active) == false && active) {
                for (var indexShard : indexService) {
                    indexShard.tryWithEngineOrNull(engine -> {
                        if (engine instanceof SearchEngine e) e.warmReaderCacheAfterResharding();
                        return null;
                    });
                }
            }
        });
    }

    private static boolean hasActiveSplit(IndexMetadata indexMetadata) {
        return indexMetadata.getReshardingMetadata() != null && indexMetadata.getReshardingMetadata().isSplit();
    }
}
