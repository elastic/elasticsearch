/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.compute.test.spill.SpillStoreConformanceTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * The conformance suite against the disk backend, built the way the plugin builds it: from the settings, a data path
 * and the I/O pool.
 */
public class DiskSpillStoreTests extends SpillStoreConformanceTestCase {

    static SpillStoreServices services(Path spillPath, Path dataPath, long capBytes, Executor io) {
        // "unlimited" is a cap no conformance test reaches; the free-space margin it implies is consulted only after a
        // few megabytes were admitted, which no conformance test writes either
        Settings settings = Settings.builder()
            .put(DiskSpillSettings.PATH.getKey(), spillPath.toString())
            .put(DiskSpillSettings.CAP.getKey(), capBytes == Long.MAX_VALUE ? "1tb" : capBytes + "b")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP));
        return new SpillStoreServices(settings, clusterSettings, List.of(dataPath), "node-" + randomAlphaOfLength(4), io);
    }

    @Override
    protected SpillStore newStore(long limitBytes) {
        return new DiskSpillPlugin().create(services(createTempDir(), createTempDir(), limitBytes, ioExecutor()));
    }

    @Override
    protected String limitSettingName() {
        return DiskSpillSettings.CAP.getKey();
    }
}
