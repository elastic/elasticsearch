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
 * Marker leaf node that represents an exact dataset name being looked up on linked projects (CPS) as
 * a potential <em>remote index</em> with the same name as a local dataset. It is the dataset analog of
 * {@link ViewShadowRelation}: where a view-name shadow rides next to the view's recursive substitution
 * inside a {@link ViewUnionAll}, a dataset-name shadow rides next to the dataset's
 * {@link UnresolvedExternalRelation} inside the plain {@link UnionAll} the
 * {@link org.elasticsearch.xpack.esql.datasources.DatasetRewriter} builds.
 * <p>
 * Motivation: {@code FROM ds} where {@code ds} is BOTH a local dataset AND a remote dataset/index.
 * The local dataset is consumed before field-caps (rewritten to an external relation), and
 * {@code DatasetRewriter.crossProjectPatternsToPreserve} only re-emits a sibling for <em>wildcards</em>
 * — an exact name returns nothing, so without this shadow the remote half of the exact name never
 * reaches field-caps. Per CPS Principle 1, an unqualified exact name expands to every container across
 * origin + all linked projects, so a remote <em>index</em> {@code ds} must federate in and a remote
 * <em>dataset/view</em> {@code ds} must FAIL.
 * <p>
 * Lifecycle, mirroring {@link ViewShadowRelation}:
 * <ol>
 *   <li>Emitted during dataset rewriting ({@code DatasetRewriter.rewriteOne}) alongside the dataset's
 *       external relation, only when CPS is enabled and the dataset was named by an exact (non-wildcard,
 *       flat) pattern. The shadow and its external sibling live in a plain {@link UnionAll}.</li>
 *   <li>{@code PreAnalyzer} collects {@code DatasetShadowRelation} patterns into the same linked-indices
 *       set {@link ViewShadowRelation} lands in, keyed by {@link #linkedIndexPattern()}.</li>
 *   <li>{@code EsqlSession.preAnalyzeLinkedIndices} issues a lenient flat field-caps request per pattern
 *       scoped to linked projects. The remote field-caps fan-out carries {@code resolveViews(true)} +
 *       {@code resolveDatasets(true)} unconditionally (see {@code EsqlResolveFieldsAction}), so a linked
 *       project that has a <em>dataset or view</em> of the same name fails the query with
 *       {@code RemoteDatasetNotSupportedException}/{@code RemoteViewNotSupportedException}; a linked
 *       project that has an <em>index</em> of the same name resolves. Results land in
 *       {@code AnalyzerContext.linkedResolution}, keyed by this shadow's {@link #linkedIndexPattern()}.</li>
 *   <li>The {@code ResolveDatasetShadow} analyzer rule (sibling of {@code ResolveViewShadow}, in the
 *       Initialize batch) consults {@code AnalyzerContext.linkedResolution} for this shadow's
 *       {@link #linkedIndexPattern()}. If a remote <em>index</em> is found the shadow is replaced with a
 *       corresponding {@code EsRelation}; otherwise the shadow is left unresolved.</li>
 *   <li>{@code StripDatasetShadowRelations} runs right after {@code ResolveDatasetShadow}: any
 *       still-unresolved shadow is removed from its {@link UnionAll}; a single-survivor union collapses
 *       to its lone child. Mirrors {@code ViewCompaction.stripViewShadowRelations} but over the plain
 *       {@link UnionAll} the dataset path produces (not a {@link ViewUnionAll}).</li>
 * </ol>
 */
public class DatasetShadowRelation extends LeafPlan implements Unresolvable {

    private final String datasetName;
    private final Kind kind;
    private final String pattern;

    public DatasetShadowRelation(Source source, String datasetName, Kind kind, String pattern) {
        super(source);
        this.datasetName = datasetName;
        this.kind = kind;
        this.pattern = pattern;
    }

    public String datasetName() {
        return datasetName;
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
    protected NodeInfo<DatasetShadowRelation> info() {
        return NodeInfo.create(this, DatasetShadowRelation::new, datasetName, kind, pattern);
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
        return "dataset-shadow lookup [" + pattern + "] not yet resolved";
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), datasetName, kind, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DatasetShadowRelation other = (DatasetShadowRelation) obj;
        return Objects.equals(datasetName, other.datasetName) && Objects.equals(kind, other.kind) && Objects.equals(pattern, other.pattern);
    }

    @Override
    public List<Object> nodeProperties() {
        return List.of(datasetName, kind, pattern);
    }

    @Override
    public String toString() {
        return "?dataset-shadow[" + pattern + "]";
    }
}
