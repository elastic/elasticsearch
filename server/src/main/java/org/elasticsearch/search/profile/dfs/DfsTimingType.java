/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.dfs;

import java.util.Locale;

public enum DfsTimingType {
    TERM_STATISTICS,
    COLLECTION_STATISTICS,
    CREATE_WEIGHT,
    REWRITE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
