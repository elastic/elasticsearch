/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public final class SearchHitUtils {

    public static String qualifiedIndex(SearchHit hit) {
        return buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex());
    }
}
