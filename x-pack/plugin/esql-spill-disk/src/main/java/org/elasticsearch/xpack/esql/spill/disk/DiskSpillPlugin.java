/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreProvider;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

/**
 * The local disk backend for ES|QL spilling, loaded by the ES|QL plugin as an extension. Stateless: the extension
 * loader may instantiate it more than once, and the store it creates belongs to the ES|QL plugin.
 */
public class DiskSpillPlugin extends Plugin implements SpillStoreProvider {

    public static final String NAME = "disk";

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(DiskSpillSettings.PATH, DiskSpillSettings.CAP);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SpillStore create(SpillStoreServices services) {
        return DiskSpillStore.create(services);
    }
}
