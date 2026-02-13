/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Contains an index pattern together with its {@link Source}. Can also be a comma-separated list, like {@code idx-*,remote:other-idx*}.
 */
public class IndexPattern {

    private final Source source;
    private final String indexPattern;

    public IndexPattern(Source source, String indexPattern) {
        this.source = source;
        this.indexPattern = indexPattern;
    }

    public String indexPattern() {
        return indexPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IndexPattern other = (IndexPattern) obj;
        return Objects.equals(indexPattern, other.indexPattern);
    }

    public Source source() {
        return source;
    }

    @Override
    public String toString() {
        return indexPattern;
    }
}
