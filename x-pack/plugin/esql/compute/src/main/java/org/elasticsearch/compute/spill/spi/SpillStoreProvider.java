/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

/**
 * One per backend module, discovered through the ES|QL plugin's extension loading. Stateless: the extension loader
 * instantiates it, possibly more than once, and the plugin owns the store it creates.
 */
public interface SpillStoreProvider {

    /**
     * The name {@code esql.spill.store} selects.
     */
    String name();

    /**
     * Called once by the plugin, which closes the store at node shutdown.
     */
    SpillStore create(SpillStoreServices services);
}
