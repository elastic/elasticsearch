/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import java.util.Arrays;

public class ParsedScrollId {

    public static final String QUERY_THEN_FETCH_TYPE = "queryThenFetch";

    public static final String QUERY_AND_FETCH_TYPE = "queryAndFetch";

    private final String source;

    private final String type;

    private final SearchContextIdForNode[] context;

    ParsedScrollId(String source, String type, SearchContextIdForNode[] context) {
        this.source = source;
        this.type = type;
        this.context = context;
    }

    public String getSource() {
        return source;
    }

    public String getType() {
        return type;
    }

    public SearchContextIdForNode[] getContext() {
        return context;
    }

    public boolean hasLocalIndices() {
        return Arrays.stream(context).anyMatch(c -> c.getClusterAlias() == null);
    }
}
