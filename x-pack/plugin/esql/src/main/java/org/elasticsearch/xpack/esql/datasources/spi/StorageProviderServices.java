/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.concurrent.ExecutorService;

/**
 * Node-level services threaded into {@link DataSourcePlugin#storageProviders(StorageProviderServices)}.
 *
 * <p>Exists so that a {@link DataSourcePlugin} can build storage providers that need node context
 * (e.g. resolving operator-managed token symlinks under {@code ${ES_PATH_CONF}} or watching them for
 * rotation) without smuggling that context across the two distinct plugin instances ESQL creates: the
 * node {@code Plugin} instance receives {@code createComponents}, while a separate reflectively built
 * SPI-discovery instance is the one whose {@code storageProviders} actually runs.
 *
 * @param settings              node settings
 * @param executor              general-purpose executor for SPI coordination and async-I/O plugin
 *                              callbacks (e.g. the HTTP client); the {@code GENERIC} pool. NOT the file-read
 *                              path — blocking reads use the dedicated {@code esql_external_blocking_io} pool
 *                              via {@code OperatorFactoryRegistry#fileReadExecutor}
 * @param environment           node {@link Environment}; {@code null} in tests that do not supply one
 * @param resourceWatcherService node {@link ResourceWatcherService}; {@code null} in tests that do not supply one
 */
public record StorageProviderServices(
    Settings settings,
    ExecutorService executor,
    @Nullable Environment environment,
    @Nullable ResourceWatcherService resourceWatcherService
) {}
