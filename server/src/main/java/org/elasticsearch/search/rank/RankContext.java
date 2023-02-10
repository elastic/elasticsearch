/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryCollectorContext;

public interface RankContext {

    Query applyRankWrappers(Query query);
    QueryCollectorContext createQueryCollectorContext(Query query, SearchContext searchContext);

}
