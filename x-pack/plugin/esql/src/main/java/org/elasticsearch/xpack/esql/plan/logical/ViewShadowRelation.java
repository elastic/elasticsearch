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
 * Lifecycle:
 * <ol>
 *   <li>Emitted during view resolution alongside the strict (recursive) resolution, as the lenient
 *       half of the strict/lenient pair for that level.</li>
 *   <li>{@code ViewCompaction.preIndexResolution} reshapes {@code Subquery}/{@code UnionAll}
 *       structures into {@link ViewUnionAll} but leaves shadows in place.</li>
 *   <li>{@code PreAnalyzer} collects shadow patterns and {@code EsqlSession} runs a lenient
 *       field-caps pass over them (linked projects only — {@code IndexResolver.FLAT_WORLD_OPTIONS}),
 *       landing results in {@code AnalyzerContext.optionalLinkedResolution}, keyed by
 *       {@link #optionalLinkedPattern()}.</li>
 *   <li>In the Initialize batch, {@code ExcludeShadowedProjectsFromViewBody} resolves in-union
 *       shadows against that map (replacing a matched shadow with the remote index's
 *       {@code EsRelation}) and removes the owning projects from the paired view body;
 *       {@code ResolveViewShadow} resolves any standalone shadow not inside a {@link ViewUnionAll}.</li>
 *   <li>{@code ViewCompactionPostIndexResolution} strips any still-unresolved shadow, then flattens
 *       nested {@link ViewUnionAll}s and unwraps {@code NamedSubquery} wrappers. Per Strategy A in
 *       <a href="https://github.com/elastic/esql-planning/issues/543">esql-planning#543</a>, sibling
 *       {@code EsRelation}s stay separate rather than being merged via a combined field-caps call.</li>
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

    /**
     * Suffix appended to a view name to key its shadow branch inside the per-level
     * {@link ViewUnionAll}, distinguishing it from the strict view-body branch (keyed by the bare
     * view name) so both can coexist in the named-subqueries map. It is purely a map-key
     * disambiguator: the {@code Analyzer} pairs a shadow with its body by the view name the shadow
     * carries ({@link #viewName()}), not by parsing this suffix.
     */
    public static final String NAME_SUFFIX = "#shadow";

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
