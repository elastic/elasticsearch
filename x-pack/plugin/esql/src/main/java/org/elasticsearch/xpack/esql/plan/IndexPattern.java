/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Contains an index pattern together with its {@link Source}. Can also be a comma-separated list, like {@code idx-*,remote:other-idx*}.
 */
public class IndexPattern implements NodeStringRenderable {

    private final Source source;
    private final String indexPattern;
    private final boolean insideView;

    public IndexPattern(Source source, String indexPattern) {
        this(source, indexPattern, false);
    }

    private IndexPattern(Source source, String indexPattern, boolean insideView) {
        this.source = source;
        this.indexPattern = indexPattern;
        this.insideView = insideView;
    }

    /**
     * The same pattern, scoped to a view body. A view is a virtual index whose behaviour must not depend on the caller's query
     * settings, so a view's relation is resolved on its own even when the caller names the very same pattern (see
     * {@code EsqlSession}): that is why the scope is part of this pattern's identity, which is what resolutions are keyed by.
     */
    public IndexPattern insideView() {
        return insideView ? this : new IndexPattern(source, indexPattern, true);
    }

    /** Whether this pattern belongs to a relation inside a view body. See {@link #insideView()}. */
    public boolean isInsideView() {
        return insideView;
    }

    public Source source() {
        return source;
    }

    public String indexPattern() {
        return indexPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, insideView);
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
        return Objects.equals(indexPattern, other.indexPattern) && insideView == other.insideView;
    }

    /**
     * Routes the index pattern through {@link NodeStringMapper#index}. Under
     * {@link NodeStringMapper#IDENTITY} this is the raw pattern (matching {@link #toString()}); under
     * an anonymizing mapper it is an index token. Lets the reflective node walk render the pattern
     * with no per-node {@code nodeString} override.
     */
    @Override
    public void nodeString(StringBuilder sb, Node.NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(mapper.index(indexPattern));
    }

    @Override
    public String toString() {
        return indexPattern;
    }
}
