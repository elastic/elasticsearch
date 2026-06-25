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
 * a potential <em>remote index</em> with the same name as a local dataset. The dataset analog of
 * {@link ViewShadowRelation}: it rides next to the dataset's {@link UnresolvedExternalRelation} inside
 * the plain {@link UnionAll} the {@link org.elasticsearch.xpack.esql.datasources.DatasetRewriter} builds.
 * <p>
 * Motivation: {@code FROM ds} where {@code ds} is BOTH a local dataset AND a remote dataset/index. The
 * local dataset is consumed before field-caps, and {@code DatasetRewriter.crossProjectPatternsToPreserve}
 * only re-emits a sibling for <em>wildcards</em> — an exact name returns nothing, so without this shadow
 * the remote half of the exact name never reaches field-caps.
 * <p>
 * Lifecycle, mirroring {@link ViewShadowRelation}:
 * <ol>
 *   <li>Emitted during dataset rewriting ({@code DatasetRewriter.rewriteOne}) alongside the dataset's
 *       external relation, only under CPS for an exact (non-wildcard, flat) pattern.</li>
 *   <li>{@code PreAnalyzer} collects the pattern into the same linked-indices set {@link ViewShadowRelation}
 *       lands in, keyed by {@link #linkedIndexPattern()}.</li>
 *   <li>{@code EsqlSession.preAnalyzeLinkedIndices} issues a lenient flat field-caps request per pattern;
 *       a linked index of the same name resolves, a linked dataset/view fails on the detect rail. Results
 *       land in {@code AnalyzerContext.linkedResolution}, keyed by {@link #linkedIndexPattern()}.</li>
 *   <li>The {@code ResolveDatasetShadow} analyzer rule (sibling of {@code ResolveViewShadow}) replaces the
 *       shadow with an {@code EsRelation} on a valid resolution, else leaves it unresolved.</li>
 *   <li>{@code StripDatasetShadowRelations} removes any still-unresolved shadow; a single-survivor union
 *       collapses to its lone child.</li>
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
