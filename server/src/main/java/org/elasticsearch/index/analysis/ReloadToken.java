/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

/**
 * Opaque per-node, per-request marker used to deduplicate reloads of shared analyzers. A single
 * {@code _reload_search_analyzers} request creates one instance per node and passes it to every
 * shard operation on that node; the registry rebuilds an analyzer shared by several indices once
 * per request rather than once per index (see {@link ReloadableCustomAnalyzer#reload}).
 *
 * <p>Compared by identity — each request gets a fresh instance — so it is deliberately a plain
 * class, not a record (whose no-field instances would all be {@code equals}). A {@code null} token
 * means "always reload" and is used by internal / direct callers such as shard recovery.
 */
public final class ReloadToken {}
