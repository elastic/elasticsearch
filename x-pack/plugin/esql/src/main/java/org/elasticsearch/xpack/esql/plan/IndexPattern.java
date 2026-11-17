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
    private final ViewScope viewScope;
    // Part of the identity: an EQL source resolves its pattern unfiltered, apart from a FROM on the same pattern that a filter narrows.
    private final boolean eqlSource;

    /**
     * How a pattern relates to view bodies, as far as unmapped-field resolution is concerned. Part of the pattern's
     * identity: resolutions are keyed by pattern, and two scopes that resolve differently must not share one.
     */
    private enum ViewScope {
        /** The caller's own pattern: the query's {@code unmapped_fields} decides its resolution. */
        QUERY,
        /** Inside a view body whose definition does not load unmapped fields: resolved as if the setting were DEFAULT. */
        VIEW,
        /** Inside a view body whose definition {@code SET unmapped_fields} to a loading mode: tracked like a loading caller's. */
        VIEW_LOADS_UNMAPPED
    }

    public IndexPattern(Source source, String indexPattern) {
        this(source, indexPattern, ViewScope.QUERY, false);
    }

    private IndexPattern(Source source, String indexPattern, ViewScope viewScope, boolean eqlSource) {
        this.source = source;
        this.indexPattern = indexPattern;
        this.viewScope = viewScope;
        this.eqlSource = eqlSource;
    }

    /**
     * The same pattern, scoped to a view body. A view is a virtual index whose behaviour must not depend on the caller's query
     * settings, so a view's relation is resolved on its own even when the caller names the very same pattern (see
     * {@code EsqlSession}): that is why the scope is part of this pattern's identity, which is what resolutions are keyed by.
     *
     * @param loadsUnmappedFields whether the view's own definition {@code SET unmapped_fields} to a loading mode, in
     *                            which case this pattern's resolution tracks unmapped field indices like a loading
     *                            caller's would
     */
    public IndexPattern insideView(boolean loadsUnmappedFields) {
        ViewScope scope = loadsUnmappedFields ? ViewScope.VIEW_LOADS_UNMAPPED : ViewScope.VIEW;
        return viewScope == scope ? this : new IndexPattern(source, indexPattern, scope, eqlSource);
    }

    public IndexPattern forEqlSource() {
        return eqlSource ? this : new IndexPattern(source, indexPattern, viewScope, true);
    }

    public boolean isEqlSource() {
        return eqlSource;
    }

    /** Whether this pattern belongs to a relation inside a view body. See {@link #insideView(boolean)}. */
    public boolean isInsideView() {
        return viewScope != ViewScope.QUERY;
    }

    /** Whether this pattern's resolution must track unmapped field indices because of its view's own settings. */
    public boolean viewLoadsUnmappedFields() {
        return viewScope == ViewScope.VIEW_LOADS_UNMAPPED;
    }

    public Source source() {
        return source;
    }

    public String indexPattern() {
        return indexPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, viewScope, eqlSource);
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
        return Objects.equals(indexPattern, other.indexPattern) && viewScope == other.viewScope && eqlSource == other.eqlSource;
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
