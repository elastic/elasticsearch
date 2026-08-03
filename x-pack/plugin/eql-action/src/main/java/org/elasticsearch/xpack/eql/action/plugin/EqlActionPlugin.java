/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action.plugin;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

/**
 * Hosts the shared EQL search transport DTOs ({@code EqlSearchAction}/{@code Request}/{@code Response}). Implements
 * {@link ExtensiblePlugin} so both x-pack-eql (the search handler) and x-pack-esql (the EQL source command) can declare
 * it in their {@code extendedPlugins} and load a single copy of those classes at runtime, avoiding a duplicate-class
 * conflict across the two plugin classloaders.
 */
public class EqlActionPlugin extends Plugin implements ExtensiblePlugin {}
