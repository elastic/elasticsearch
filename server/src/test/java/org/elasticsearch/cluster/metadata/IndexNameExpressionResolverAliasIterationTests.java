/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

public class IndexNameExpressionResolverAliasIterationTests extends IndexNameExpressionResolverTests {

    protected IndexNameExpressionResolver createIndexNameExpressionResolver() {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)) {
            @Override
            boolean iterateIndexAliases(int indexAliasesSize, int resolvedExpressionsSize) {
                return true;
            }
        };
    }

}
