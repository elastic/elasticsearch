/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern;
import org.elasticsearch.xpack.esql.plan.LinkedIndexPattern.Kind;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Marker leaf node that represents a view-name being looked up on linked projects (CPS) as a
 * potential <em>remote index</em> with the same name as a local view. It is emitted by
 * {@link org.elasticsearch.xpack.esql.view.ViewResolver} (only when CPS is enabled) as a sibling
 * of each resolved view's recursive substitution, inside the per-resolution-level
 * {@link ViewUnionAll}.
 * <p>
 * Lifecycle, with the parts each PR is responsible for:
 * <ol>
 *   <li>Emitted during view resolution alongside the strict (recursive) resolution. The shadow
 *       and its strict sibling form the strict/lenient pair for that level.
 *       <em>(landed in the {@code ViewResolver} refactor)</em></li>
 *   <li>{@code ViewCompaction.preIndexResolution} reshapes user-written {@code Subquery}/
 *       {@code UnionAll} structures into {@link ViewUnionAll} but leaves shadows in place so
 *       PreAnalyzer can still pair each shadow with its sibling at index-resolution time.
 *       <em>(landed)</em></li>
 *   <li>{@code PreAnalyzer} collects {@code ViewShadowRelation} patterns into a separate set,
 *       and {@code EsqlSession} issues a lenient field-caps request per batch
 *       ({@code ALLOW_UNAVAILABLE_TARGETS} + project routing scoped to linked projects only —
 *       {@code IndexResolver.FLAT_WORLD_OPTIONS}). Results land in
 *       {@code AnalyzerContext.optionalLinkedResolution}, keyed by the shadow's full
 *       {@link #linkedIndexPattern()} (view name + applicable exclusions).
 *       <em>(deferred to the lenient field-caps PR)</em></li>
 *   <li>The {@code ResolveViewShadow} analyzer rule (sibling of {@code ResolveTable}, in the
 *       Initialize batch) consults {@code AnalyzerContext.optionalLinkedResolution} for this shadow's
 *       {@link #linkedIndexPattern()}. If a remote <em>index</em> is found the shadow is replaced
 *       with a corresponding {@code EsRelation}; otherwise the shadow is left unresolved.
 *       <em>(this PR — backed by a mocked {@code optionalLinkedResolution} map until the lenient
 *       field-caps PR provides real data)</em></li>
 *   <li>{@code ViewCompactionPostIndexResolution} runs after {@code ResolveViewShadow}: any
 *       still-unresolved shadow is stripped, then nested {@link ViewUnionAll}s are flattened
 *       and remaining {@code NamedSubquery} wrappers unwrapped. Per Strategy A in
 *       <a href="https://github.com/elastic/esql-planning/issues/543">esql-planning#543</a>,
 *       sibling {@code EsRelation}s stay separate (a {@code UnionAll}/{@link ViewUnionAll}
 *       of {@code EsRelation}s) rather than being merged via a third combined field-caps call.
 *       <em>(landed)</em></li>
 * </ol>
 * The strict, default-options field-caps path on the local cluster keeps {@code resolveViews(true)}
 * unchanged, so a remote project that has a <em>view</em> with the same name still fails the query
 * with {@code RemoteViewNotSupportedException}. This node only enables lookup of remote
 * <em>indices</em> with the same name as a local view.
 */
public class ViewShadowRelation extends LeafPlan implements Unresolvable {

    private final String viewName;
    private final Kind kind;
    private final String pattern;

    public ViewShadowRelation(Source source, String viewName, Kind kind, String pattern) {
        super(source);
        this.viewName = viewName;
        this.kind = kind;
        this.pattern = pattern;
    }

    public String viewName() {
        return viewName;
    }

    public LinkedIndexPattern linkedIndexPattern() {
        return new LinkedIndexPattern(kind, new IndexPattern(source(), pattern));
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<ViewShadowRelation> info() {
        return NodeInfo.create(this, ViewShadowRelation::new, viewName, kind, pattern);
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public String unresolvedMessage() {
        return "view-shadow lookup [" + pattern + "] not yet resolved";
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), viewName, kind, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ViewShadowRelation other = (ViewShadowRelation) obj;
        return Objects.equals(viewName, other.viewName) && Objects.equals(kind, other.kind) && Objects.equals(pattern, other.pattern);
    }

    @Override
    public List<Object> nodeProperties() {
        return List.of(viewName, kind, pattern);
    }

    @Override
    public String toString() {
        return "?shadow[" + pattern + "]";
    }
}
