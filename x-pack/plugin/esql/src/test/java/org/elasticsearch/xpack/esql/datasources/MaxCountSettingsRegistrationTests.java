/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetService;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The ceilings on how many data sources and datasets a project may hold travel with the federation feature, so on a node
 * where an operator unregistered it (see {@link Federation#settings}) those settings do not exist. {@link DataSourceService}
 * and {@link DatasetService} are built on every node regardless, because the CRUD transport actions inject them, so each
 * must come up against cluster settings that do not know its ceiling rather than taking the node down at startup.
 */
public class MaxCountSettingsRegistrationTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testServicesStartWithoutTheirCeilingSettings() {
        try (ClusterService clusterService = clusterService(Set.of())) {
            new DataSourceService(clusterService, Map.of(), new PassThroughEncryptionService());
            new DatasetService(clusterService, Map.of());
        }
    }

    public void testServicesStartWithTheirCeilingSettings() {
        try (ClusterService clusterService = clusterService(Set.copyOf(Federation.settings(true)))) {
            new DataSourceService(clusterService, Map.of(), new PassThroughEncryptionService());
            new DatasetService(clusterService, Map.of());
        }
    }

    /**
     * Where the settings exist they stay live: a ceiling reaches its consumer on registration and again on every update.
     * Asserted through a consumer of this test's own rather than the services' private field, which also pins what the
     * services rely on, namely that both ceilings are dynamic. A ceiling that stopped being dynamic would quietly become
     * a start-up-only value instead of failing.
     */
    public void testRegisteredCeilingsAreWatched() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.copyOf(Federation.settings(true)));
        assertCeilingIsWatched(clusterSettings, DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING, 7);
        assertCeilingIsWatched(clusterSettings, DatasetService.MAX_DATASETS_COUNT_SETTING, 13);
    }

    private static void assertCeilingIsWatched(ClusterSettings clusterSettings, Setting<Integer> ceiling, int updated) {
        AtomicInteger observed = new AtomicInteger();
        clusterSettings.initializeAndWatchIfRegistered(ceiling, observed::set);
        assertEquals(
            "[" + ceiling.getKey() + "] must be initialized to its default",
            ceiling.getDefault(Settings.EMPTY).intValue(),
            observed.get()
        );

        clusterSettings.applySettings(Settings.builder().put(ceiling.getKey(), updated).build());
        assertEquals("[" + ceiling.getKey() + "] must follow an update", updated, observed.get());
    }

    private ClusterService clusterService(Set<Setting<?>> esqlSettings) {
        // The built-in settings are what ClusterService itself watches; the federation settings are the ones under test.
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.addAll(esqlSettings);
        return ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(Settings.EMPTY, settings));
    }

    /**
     * The services only hold on to the encryption service, so an implementation that hands the bytes back is enough here.
     * The encrypt transform itself is covered by {@code DataSourceServiceEncryptionTests}.
     */
    private static class PassThroughEncryptionService implements EncryptionService {
        @Override
        public EncryptedData encrypt(byte[] bytes) {
            return new EncryptedData("test-key", bytes.clone());
        }

        @Override
        public byte[] decrypt(EncryptedData encryptedData) {
            return encryptedData.payload();
        }
    }
}
