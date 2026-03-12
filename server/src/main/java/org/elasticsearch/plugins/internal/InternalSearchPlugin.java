/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.plugins.internal.rewriter.QueryRewriteInterceptor;

import java.util.List;

import static java.util.Collections.emptyList;

public interface InternalSearchPlugin {

    /**
     * @return Applicable {@link QueryRewriteInterceptor}s configured for this plugin.
     * Note: This is internal to Elasticsearch's API and not extensible by external plugins.
     */
    default List<QueryRewriteInterceptor> getQueryRewriteInterceptors() {
        return emptyList();
    }
}
