/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreProvider;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The store is chosen by name among the loaded providers: a name no provider answers to fails the node, the default with
 * no provider means no store.
 */
public class SpillStoresTests extends ESTestCase {

    private static SpillStoreProvider provider(String name, SpillStore store) {
        return new SpillStoreProvider() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public SpillStore create(SpillStoreServices services) {
                return store;
            }
        };
    }

    private static SpillStoreServices services(Settings settings) {
        return new SpillStoreServices(
            settings,
            new ClusterSettings(settings, Set.of(SpillSettings.STORE)),
            List.of(),
            "node",
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
    }

    private MemorySpillStore memory() {
        return new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, 1 << 20, "memory.limit", random());
    }

    public void testTheNamedProviderIsSelected() {
        MemorySpillStore memory = memory();
        SpillStore store = SpillStores.create(
            List.of(provider("other", memory()), provider("memory", memory)),
            services(Settings.builder().put(SpillSettings.STORE.getKey(), "memory").build())
        );
        assertThat(store, sameInstance(memory));
    }

    public void testTheDefaultWithoutAProviderIsNoStore() {
        assertNull(SpillStores.create(List.of(), services(Settings.EMPTY)));
        assertNull(SpillStores.create(List.of(provider("memory", memory())), services(Settings.EMPTY)));
    }

    public void testAnExplicitNameNoProviderAnswersToFailsTheNode() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SpillStores.create(
                List.of(provider("memory", memory())),
                services(Settings.builder().put(SpillSettings.STORE.getKey(), "nope").build())
            )
        );
        assertThat(e.getMessage(), containsString(SpillSettings.STORE.getKey()));
        assertThat(e.getMessage(), containsString("[nope]"));
        assertThat(e.getMessage(), containsString("memory"));
    }

    public void testShutdownWaitsForTheStoreAndTakesNoStore() {
        SpillStores.shutdown(memory(), TimeValue.timeValueSeconds(5));
        SpillStores.shutdown(null, TimeValue.timeValueSeconds(5));
    }
}
