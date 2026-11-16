/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreProvider;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Selects the backend {@code esql.spill.store} names among the loaded providers and builds it. The provider is stateless
 * and instantiated by the extension loader; the plugin owns the store it creates and shuts it down at node close.
 */
public final class SpillStores {

    private static final Logger logger = LogManager.getLogger(SpillStores.class);

    private SpillStores() {}

    /**
     * The store, or null when the setting is at its default and no provider is present. An explicit name that no provider
     * answers to fails the node at start, with a message naming the setting, rather than running without a store.
     */
    @Nullable
    public static SpillStore create(List<SpillStoreProvider> providers, SpillStoreServices services) {
        Settings settings = services.settings();
        String name = SpillSettings.STORE.get(settings);
        for (SpillStoreProvider provider : providers) {
            if (name.equals(provider.name())) {
                SpillStore store = provider.create(services);
                logger.info("spill store [{}] ready; limit [{}] bytes", name, store.bytesLimit());
                return store;
            }
        }
        if (SpillSettings.STORE.exists(settings)) {
            throw new IllegalArgumentException(
                "[" + SpillSettings.STORE.getKey() + "] names [" + name + "] but no such spill store is installed; have " + names(providers)
            );
        }
        logger.info("no spill store [{}] installed, providers present {}; spilling is off", name, names(providers));
        return null;
    }

    private static List<String> names(List<SpillStoreProvider> providers) {
        return providers.stream().map(SpillStoreProvider::name).toList();
    }

    /**
     * Shuts the store down and waits a bounded time for it, since the node's close is synchronous; a late or failed
     * shutdown is logged, the sweep at the next start removes what it left.
     */
    public static void shutdown(@Nullable SpillStore store, TimeValue timeout) {
        if (store == null) {
            return;
        }
        PlainActionFuture<Void> done = new PlainActionFuture<>();
        store.shutdown(ActionListener.wrap(done::onResponse, e -> {
            logger.warn("spill store shutdown failed; the next start sweeps what it left", e);
            done.onResponse(null);
        }));
        try {
            done.get(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted while the spill store shut down; the next start sweeps what it left");
        } catch (Exception e) {
            logger.warn("spill store shutdown did not complete within [{}]; the next start sweeps what it left", timeout);
        }
    }
}
