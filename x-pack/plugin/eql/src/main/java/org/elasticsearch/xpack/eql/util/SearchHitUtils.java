/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.util.StringUtils;

import static org.elasticsearch.common.Strings.hasText;

public final class SearchHitUtils {

    public static String qualifiedIndex(SearchHit hit) {
        String qualification = hasText(hit.getClusterAlias()) ? hit.getClusterAlias() + ":" : StringUtils.EMPTY;
        return qualification + hit.getIndex();
    }
}
