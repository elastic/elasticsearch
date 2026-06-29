/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;

/**
 * First-class representation of a {@code FROM <dataset>} reference in the logical plan — the boundary today's
 * {@code DatasetRewriter} erases by rewriting the dataset straight into the shared {@code EXTERNAL}
 * {@link UnresolvedExternalRelation}. Giving the dataset its own node (distinct from the inline-{@code EXTERNAL} path,
 * which stays on {@link ExternalRelation}) lets a <em>remote</em> dataset be lowered opaquely, the same way the
 * {@link View} node lets a remote view be lowered opaquely.
 * <p>
 * For a <b>local</b> dataset this is a <b>unary</b> node wrapping the dataset's relation as its child: at parse-time the
 * child is the {@link UnresolvedExternalRelation} the dataset rewrite still produces, and the existing analyzer
 * resolution chain ({@code PreAnalyzer}'s {@code forEachUp(UnresolvedExternalRelation.class, ...)} path collection and
 * the {@code ResolveExternalRelations} rule) sees through this wrapper and resolves the child into the <em>identical</em>
 * {@link ExternalRelation} it produces today — both descend into a child but not into a leaf's hidden field, so wrapping
 * is transparent to resolution. This is the {@link View} parity mechanism, mirrored for datasets. A
 * {@link Boundary#REMOTE} / {@link Boundary#MATERIALIZED} dataset is constructed directly with a target (the relation
 * child carries only its schema); its body never executes locally.
 * <p>
 * Unlike a view, a dataset has no inline-vs-not decision, so there is <b>no optimizer fold rule</b>: the {@code Dataset}
 * node survives the optimizer untouched and the {@code Mapper} lowers it boundary-aware. A {@link Boundary#LOCAL} dataset
 * lowers to exactly the external read its child produces today (the parity anchor); {@link Boundary#REMOTE} /
 * {@link Boundary#MATERIALIZED} lower to first-class physical execs.
 * <p>
 * The model is additive: a 4th boundary slots in by adding a {@link Boundary} constant, a {@link LoweringTarget} field
 * for its per-mode data, and a {@code Mapper} case — no change to the existing three.
 * <p>
 * <b>Transient:</b> every {@code Dataset} is lowered before physical mapping, so it never crosses the wire —
 * {@link #writeTo} throws and the node is not registered in {@code PlanWritables}.
 */
public class Dataset extends UnaryPlan {

    /**
     * Where a dataset's data lives — the execution-shape decision the dataset carries as a first-class logical node into
     * the {@code Mapper}, which lowers it differently per boundary. Mirrors {@link View.Boundary}; kept a distinct type so
     * datasets and views stay independent (symmetric, not coupled).
     */
    public enum Boundary {
        /** Data is read locally — lowered to exactly the external read the relation child produces today. */
        LOCAL,
        /** Data lives on a remote cluster — must NOT be read locally; lowered to {@code RemoteDatasetExec}. */
        REMOTE,
        /** Results live in a precomputed backing store — lowered to {@code MaterializedDatasetExec}. */
        MATERIALIZED
    }

    /**
     * The per-mode data a non-{@link Boundary#LOCAL} dataset needs to be lowered. {@code LOCAL} carries no target (its
     * data is its relation child). {@code REMOTE} carries the {@code handle} (home cluster from the federation
     * {@code resolve_schema} seam); {@code MATERIALIZED} carries the {@code backingIndex} ref. Kept as one carrier so a
     * 4th boundary adds a field here rather than another constructor parameter on {@code Dataset}.
     */
    public record LoweringTarget(String handle, String backingIndex) {
        public static LoweringTarget remote(String handle) {
            return new LoweringTarget(handle, null);
        }

        public static LoweringTarget materialized(String backingIndex) {
            return new LoweringTarget(null, backingIndex);
        }
    }

    private final String name;
    private final Boundary boundary;
    private final LoweringTarget loweringTarget;

    /** Creates a {@link Boundary#LOCAL} dataset — what {@code DatasetRewriter} produces for every dataset today. */
    public Dataset(Source source, String name, LogicalPlan relation) {
        this(source, name, relation, Boundary.LOCAL, null);
    }

    /**
     * @param relation       the dataset's relation; for {@code LOCAL} the {@code UnresolvedExternalRelation} (later the
     *                       resolved {@link ExternalRelation}) the rewrite produces, for {@code REMOTE}/{@code MATERIALIZED}
     *                       a relation carrying only the resolved schema (its body does not execute locally)
     * @param boundary       where the dataset's data lives; {@link Boundary#LOCAL} reproduces today's external read
     * @param loweringTarget per-mode lowering data ({@code null} for {@code LOCAL}; required for {@code REMOTE}/{@code MATERIALIZED})
     */
    public Dataset(Source source, String name, LogicalPlan relation, Boundary boundary, LoweringTarget loweringTarget) {
        super(source, relation);
        this.name = name;
        this.boundary = boundary;
        this.loweringTarget = loweringTarget;
    }

    /** The dataset name as written in the query (its identity). */
    public String datasetName() {
        return name;
    }

    /** Where this dataset's data lives — drives how the {@code Mapper} lowers it. */
    public Boundary boundary() {
        return boundary;
    }

    /** The per-mode lowering data, or {@code null} for a {@link Boundary#LOCAL} dataset. */
    public LoweringTarget loweringTarget() {
        return loweringTarget;
    }

    /** The dataset's relation — the external read the {@code Mapper} maps for a {@link Boundary#LOCAL} dataset. */
    public LogicalPlan relation() {
        return child();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Dataset(source(), name, newChild, boundary, loweringTarget);
    }

    @Override
    public boolean expressionsResolved() {
        return child().expressionsResolved();
    }

    @Override
    protected NodeInfo<Dataset> info() {
        return NodeInfo.create(this, Dataset::new, name, child(), boundary, loweringTarget);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Dataset is a transient node lowered by the Mapper and must not be serialized");
    }

    @Override
    public String getWriteableName() {
        return "Dataset";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // The dataset name is an index-like identifier — route it through the mapper for anonymization.
        sb.append(nodeName()).append('[').append(mapper.index(name)).append("][").append(boundary).append(']');
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, child(), boundary, loweringTarget);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Dataset other = (Dataset) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(child(), other.child())
            && boundary == other.boundary
            && Objects.equals(loweringTarget, other.loweringTarget);
    }
}
