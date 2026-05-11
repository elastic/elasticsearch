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
 *       {@link #optionalLinkedPattern()} (view name + applicable exclusions).
 *       <em>(deferred to the lenient field-caps PR)</em></li>
 *   <li>The {@code ResolveViewShadow} analyzer rule (sibling of {@code ResolveTable}, in the
 *       Initialize batch) consults {@code AnalyzerContext.optionalLinkedResolution} for this shadow's
 *       {@link #optionalLinkedPattern()}. If a remote <em>index</em> is found the shadow is replaced
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
 * <p>
 * The {@link #exclusions()} list captures any exclusion patterns that appeared <em>after</em>
 * the view's referencing position in the parent {@code UnresolvedRelation} pattern list. These
 * travel with the lenient lookup as part of {@link #optionalLinkedPattern()} so the per-shadow field-caps
 * target is {@code viewName,exclusion1,...} — mirroring the local exclusion scope exactly. See
 * {@code refactor_view_resolver_for_cps.md} for the position-aware semantics. The exclusions
 * are part of the {@code optionalLinkedResolution} map's key (via {@link #optionalLinkedPattern()}), so the
 * same view referenced from positions with different exclusion lists yields distinct lookups
 * and may resolve differently — e.g. one position's exclusions empty out the lenient
 * field-caps target while another resolves to a remote index.
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
    public IndexPattern optionalLinkedPattern() {
        if (exclusions.isEmpty()) {
            return new IndexPattern(source(), viewName);
        }
        return new IndexPattern(source(), viewName + "," + String.join(",", exclusions));
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
        return "view-shadow lookup [" + optionalLinkedPattern() + "] not yet resolved";
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
        return "?shadow[" + optionalLinkedPattern() + "]";
    }
}
