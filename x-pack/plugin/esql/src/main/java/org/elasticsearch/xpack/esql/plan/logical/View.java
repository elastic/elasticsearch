/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * First-class representation of a {@code FROM <view>} reference in the logical plan — the boundary today's
 * {@code ViewResolver} substitution erases too early.
 * <p>
 * For a <b>local</b> view this is a <b>unary</b> node wrapping the view's resolved body as its child. Keeping the body a
 * real child (rather than a leaf-with-held-body) is what preserves parity: {@code PreAnalyzer} collects field-caps
 * patterns by walking the tree's naked relation leaves ({@code forEachUp(UnresolvedRelation.class, ...)}), and the
 * analyzer's {@code ResolveTable} resolves them — both descend into a child but not into a leaf's hidden field. So the
 * existing resolution chain sees through the {@code View} unchanged, and {@code output()} delegates to the body's output
 * (its schema). This is Spark Catalyst's {@code View} + {@code EliminateView} shape. A <b>remote</b> or
 * <b>materialized</b> view — which has no local child to wrap — is a later variant (a handle in place of the child).
 * <p>
 * The view is opaque to the optimizer only after analysis: the boundary-aware {@code InlineView} rule decides <em>how</em>
 * to lower it based on its {@link Boundary}. A {@link Boundary#LOCAL} view (the default, today's behaviour) is folded into
 * its body (the peer-through strategy) before the pushdown rules run; a {@link Boundary#REMOTE} or
 * {@link Boundary#MATERIALIZED} view keeps its boundary and is lowered to a first-class source node instead.
 * <p>
 * <b>Transient:</b> every {@code View} is lowered before physical mapping, so it never crosses the wire —
 * {@link #writeTo} throws and the node is not registered in {@code PlanWritables}. Wire-serialization (with a
 * {@code TransportVersion} gate) and the contract/rights-mode fields land in later phases.
 */
public class View extends UnaryPlan implements PostOptimizationPlanVerificationAware {

    /**
     * Where a view's body executes — the execution-shape decision the view carries as a first-class logical node into the
     * optimizer, which lowers it differently per boundary. The model is additive: a 4th boundary slots in by adding an
     * enum constant, a {@link LoweringTarget} field for its per-mode data, a branch in {@code InlineView}, and a
     * {@code Mapper} case for its lowered node — no change to the existing three.
     */
    public enum Boundary {
        /** Body executes locally — folded into its body by {@code InlineView}, exactly today's behaviour. */
        LOCAL,
        /** Body executes on a remote cluster — must NOT inline locally; lowered to {@code RemoteViewSource}. */
        REMOTE,
        /** Results live in a precomputed backing store — lowered to {@code MaterializedReadSource}. */
        MATERIALIZED
    }

    /**
     * The per-mode data a non-{@link Boundary#LOCAL} view needs to be lowered. {@code LOCAL} carries no target (its data
     * is its body child). {@code REMOTE} carries the {@code handle} (home cluster from the federation {@code resolve_schema}
     * seam); {@code MATERIALIZED} carries the {@code backingIndex} ref. Kept as one carrier so a 4th boundary adds a field
     * here rather than another constructor parameter on {@code View}.
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

    /** Creates a {@link Boundary#LOCAL} view — the default everywhere current views are created, preserving behaviour. */
    public View(Source source, String name, LogicalPlan body) {
        this(source, name, body, Boundary.LOCAL, null);
    }

    /**
     * @param boundary       where the view's body executes; {@link Boundary#LOCAL} reproduces today's inline behaviour
     * @param loweringTarget per-mode lowering data ({@code null} for {@code LOCAL}; required for {@code REMOTE}/{@code MATERIALIZED})
     */
    public View(Source source, String name, LogicalPlan body, Boundary boundary, LoweringTarget loweringTarget) {
        super(source, body);
        this.name = name;
        this.boundary = boundary;
        this.loweringTarget = loweringTarget;
    }

    /** The view name as written in the query (its identity). */
    public String viewName() {
        return name;
    }

    /** Where this view's body executes — drives how {@code InlineView} lowers it. */
    public Boundary boundary() {
        return boundary;
    }

    /** The per-mode lowering data, or {@code null} for a {@link Boundary#LOCAL} view. */
    public LoweringTarget loweringTarget() {
        return loweringTarget;
    }

    /** The resolved plan of the view's stored query — the implementation the {@code InlineView} rule folds in. */
    public LogicalPlan body() {
        return child();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new View(source(), name, newChild, boundary, loweringTarget);
    }

    @Override
    public boolean expressionsResolved() {
        // A View wraps an already-resolved body and has no expressions of its own; resolution of the body is governed
        // by childrenResolved().
        return true;
    }

    @Override
    protected NodeInfo<View> info() {
        return NodeInfo.create(this, View::new, name, child(), boundary, loweringTarget);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("View must be inlined before serialization");
    }

    @Override
    public String getWriteableName() {
        // Not registered in PlanWritables while transient; a stable name for diagnostics until the remote-view phase.
        return "View";
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        // Route the view name through the mapper so anonymization can mask it — a view name can be
        // as sensitive as an index name. A raw append here would leak it into the plan string.
        sb.append(nodeName()).append('[').append(mapper.index(name)).append(']');
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return View::checkViewFolded;
    }

    /**
     * A {@code View} is transient: the boundary-aware {@code InlineView} rule must lower every one during logical
     * optimization (LOCAL folds into its body; REMOTE/MATERIALIZED lower to a first-class source node), so none may
     * survive into physical mapping. The only other backstop is {@link #writeTo} throwing, which fires far later (at
     * serialization) and with a less actionable message. Fail loud here, mirroring {@code UnionAll#checkNestedUnionAlls},
     * if a {@code View} reaches post-optimization verification — that means {@code InlineView} was not wired or did not
     * fire, which is a planner bug, not a user error.
     */
    private static void checkViewFolded(LogicalPlan plan, Failures failures) {
        if (plan instanceof View view) {
            failures.add(Failure.fail(view, "View [{}] was not lowered by InlineView before physical mapping", view.viewName()));
        }
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
        View other = (View) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(child(), other.child())
            && boundary == other.boundary
            && Objects.equals(loweringTarget, other.loweringTarget);
    }
}
