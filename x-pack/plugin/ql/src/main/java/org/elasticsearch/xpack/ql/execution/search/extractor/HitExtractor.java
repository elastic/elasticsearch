/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.search.SearchHit;

/**
 * Extracts a column value from a {@link SearchHit}.
 */
public interface HitExtractor extends NamedWriteable {
    /**
     * Extract the value from a hit.
     */
    Object extract(SearchHit hit);

    /**
     * Name of the inner hit needed by this extractor if it needs one, {@code null} otherwise.
     */
    @Nullable
    String hitName();
}