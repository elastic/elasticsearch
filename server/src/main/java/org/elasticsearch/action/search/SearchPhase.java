/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.Objects;

/**
 * Base class for all individual search phases like collecting distributed frequencies, fetching documents, querying shards.
 */
abstract class SearchPhase {
    private static final Logger logger = LogManager.getLogger(SearchPhase.class);

    private final String name;

    protected SearchPhase(String name) {
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    protected abstract void run();

    /**
     * Returns the phases name.
     */
    public String getName() {
        return name;
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    protected static void releaseIrrelevantSearchContext(SearchPhaseResult searchPhaseResult, AsyncSearchContext<?> context) {
        // we only release search context that we did not fetch from, if we are not scrolling
        // or using a PIT and if it has at least one hit that didn't make it to the global topDocs
        // phaseResult.getContextId() is the same for query & rank feature results
        SearchPhaseResult phaseResult = searchPhaseResult.queryResult() != null
            ? searchPhaseResult.queryResult()
            : searchPhaseResult.rankFeatureResult();
        if (phaseResult != null
            && (phaseResult.hasSearchContext() || (phaseResult instanceof QuerySearchResult q && q.isReduced() && q.getContextId() != null))
            && context.getRequest().scroll() == null
            && (AsyncSearchContext.isPartOfPIT(null, context.getRequest(), phaseResult.getContextId()) == false)) {
            try {
                logger.trace("trying to release search context [{}]", phaseResult.getContextId());
                SearchShardTarget shardTarget = phaseResult.getSearchShardTarget();
                Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
                context.sendReleaseSearchContext(phaseResult.getContextId(), connection);
            } catch (Exception e) {
                logger.trace("failed to release context", e);
            }
        }
    }
}
