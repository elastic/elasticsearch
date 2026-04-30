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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Marker leaf node that represents a view-name being looked up on linked projects (CPS) as a
 * potential <em>remote index</em> with the same name as a local view. It is emitted by
 * {@link org.elasticsearch.xpack.esql.view.ViewResolver} as a sibling of each resolved view's
 * recursive substitution, inside the per-resolution-level {@link ViewUnionAll}.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Emitted during view resolution alongside the strict (recursive) resolution.</li>
 *   <li>{@code PreAnalyzer} collects {@code ViewShadowRelation} patterns into a separate set, so
 *       they go to field-caps with lenient indices options ({@code ALLOW_UNAVAILABLE_TARGETS}) and
 *       project routing scoped to linked projects only.</li>
 *   <li>A dedicated analyzer rule (sibling of {@code ResolveTable}) consults the lenient
 *       {@code IndexResolution} for this shadow's name. If a remote <em>index</em> is found, the
 *       shadow is replaced with a corresponding {@code EsRelation}; otherwise the shadow is
 *       dropped from its parent {@link ViewUnionAll}.</li>
 *   <li>The post-resolution {@code ViewCompaction} rule then lifts any nested {@link Fork}/
 *       {@link ViewUnionAll} structures and finalises the plan. Per Strategy A in
 *       <a href="https://github.com/elastic/esql-planning/issues/543">esql-planning#543</a>, sibling
 *       {@code EsRelation}s remain separate (a {@code UnionAll} of {@code EsRelation}s) rather than
 *       being merged into a single {@code EsRelation} via a third combined field-caps call.</li>
 * </ol>
 * <p>
 * The {@code exclusions} list captures any exclusion patterns that appeared <em>after</em> the
 * view's referencing position in the parent {@code UnresolvedRelation} pattern list. These travel
 * with the lenient lookup so the per-shadow field-caps target is {@code viewName,exclusion1,...}
 * — mirroring the local exclusion scope exactly. See the design table in
 * {@code refactor_view_resolver_for_cps.md} for the position-aware semantics.
 * <p>
 * The strict, default-options field-caps path on the local cluster keeps {@code resolveViews(true)}
 * unchanged, so a remote project that has a <em>view</em> with the same name still fails the query
 * with {@code RemoteViewNotSupportedException}. This node only enables lookup of remote
 * <em>indices</em> with the same name as a local view.
 */
public class ViewShadowRelation extends LeafPlan implements Unresolvable {

    private final String viewName;
    private final List<String> exclusions;

    public ViewShadowRelation(Source source, String viewName, List<String> exclusions) {
        super(source);
        this.viewName = viewName;
        this.exclusions = List.copyOf(exclusions);
    }

    public String viewName() {
        return viewName;
    }

    public List<String> exclusions() {
        return exclusions;
    }

    /**
     * The pattern to send to field-caps for the lenient lookup: view name + applicable exclusions.
     */
    public String indexPattern() {
        if (exclusions.isEmpty()) {
            return viewName;
        }
        return viewName + "," + String.join(",", exclusions);
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
        return NodeInfo.create(this, ViewShadowRelation::new, viewName, exclusions);
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
        return "view-shadow lookup [" + indexPattern() + "] not yet resolved";
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), viewName, exclusions);
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
        return Objects.equals(viewName, other.viewName) && Objects.equals(exclusions, other.exclusions);
    }

    @Override
    public List<Object> nodeProperties() {
        return List.of(viewName, exclusions);
    }

    @Override
    public String toString() {
        return "?shadow[" + indexPattern() + "]";
    }
}
