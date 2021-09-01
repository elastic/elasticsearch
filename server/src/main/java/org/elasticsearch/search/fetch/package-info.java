/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Search phase that fetches the top hits from the shards after the results of the query phase have been merged. Pluggable by implementing
 * {@link org.elasticsearch.search.fetch.FetchSubPhase} and
 * {@link org.elasticsearch.plugins.SearchPlugin#getFetchSubPhases(org.elasticsearch.plugins.SearchPlugin.FetchPhaseConstructionContext)}.
 */
package org.elasticsearch.search.fetch;
