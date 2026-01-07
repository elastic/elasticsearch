/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SortPreserving;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputExpressions;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * Specialized type of join where the source of the left and right plans are the same. The plans themselves can contain different nodes
 * however at the core, both have the same source.
 * <p>Furthermore, this type of join indicates the right side is performing a subquery identical to the left side - meaning its result is
 * required before joining with the left side.
 * <p>
 * This helps the model since we want any transformation applied to the source to show up on both sides of the join - due the immutability
 * of the tree (which uses value instead of reference semantics), even if the same node instance would be used, any transformation applied
 * on one side (which would create a new source) would not be reflected on the other side (still use the old source instance).
 * This dedicated instance handles that by replacing the source of the right with a StubRelation that simplifies copies the output of the
 * source, making it easy to serialize/deserialize as well as traversing the plan.
 */
public class InlineJoin extends Join implements SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "InlineJoin",
        InlineJoin::readFrom
    );

    /**
     * Replaces the source of the target plan with a stub preserving the output of the source plan.
     */
    public static LogicalPlan stubSource(UnaryPlan sourcePlan, LogicalPlan target) {
        return sourcePlan.replaceChild(new StubRelation(sourcePlan.source(), StubRelation.computeOutput(sourcePlan, target)));
    }

    /**
     * TODO: perform better planning
     * Keep the join in place or replace it with an Eval in case no grouping is necessary.
     */
    public static LogicalPlan inlineData(InlineJoin target, LocalRelation data) {
        if (target.config().leftFields().isEmpty()) {
            List<Attribute> schema = data.output();
            Page page = data.supplier().get();
            List<Alias> aliases = new ArrayList<>(schema.size());
            for (int i = 0; i < schema.size(); i++) {
                Attribute attr = schema.get(i);
                aliases.add(
                    new Alias(attr.source(), attr.name(), Literal.of(attr, BlockUtils.toJavaObject(page.getBlock(i), 0)), attr.id())
                );
            }
            return new Eval(target.source(), target.left(), aliases);
        } else {
            return target.replaceRight(data);
        }
    }

    @Override
    protected LogicalPlan getRightToSerialize(StreamOutput out) {
        return right();
    }

    /**
     * Replaces the stubbed source with the actual source.
     * NOTE: this will replace the first {@link StubRelation}s found with the source and the method is meant to be used to replace one node
     * only when being called on a plan that has only ONE StubRelation in it.
     */
    public static LogicalPlan replaceStub(LogicalPlan stubReplacement, LogicalPlan stubbedPlan) {
        // here we could have used stubbed.transformUp(StubRelation.class, stubRelation -> source)
        // but transformUp skips changing a node if its transformed variant is equal to its original variant.
        // A StubRelation can contain in its output ReferenceAttributes which do not use NameIds for equality, but only names and
        // two ReferenceAttributes with the same name are equal and the transformation will not be applied.
        Holder<Boolean> doneReplacing = new Holder<>(false);
        var result = stubbedPlan.transformUp(UnaryPlan.class, up -> {
            if (up.child() instanceof StubRelation) {
                if (doneReplacing.get() == false) {
                    doneReplacing.set(true);
                    return up.replaceChild(stubReplacement);
                }
                throw new IllegalStateException("Expected to replace a single StubRelation in the plan, but found more than one");
            }
            return up;
        });

        if (doneReplacing.get() == false) {
            throw new IllegalStateException("Expected to replace a single StubRelation in the plan, but none found");
        }

        return result;
    }

    /**
     * @param stubReplacedSubPlan - the completed / "destubbed" right-hand side of the bottommost InlineJoin in the plan. For example:
     *                            Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     *                            \_Limit[1000[INTEGER],false]
     *                              \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     * @param originalSubPlan - the original (unchanged) right-hand side of the bottommost InlineJoin in the plan. For example:
     *                        Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     *                        \_StubRelation[[x{r}#99]]]
     */
    public record LogicalPlanTuple(LogicalPlan stubReplacedSubPlan, LogicalPlan originalSubPlan) {}

    /**
     * Finds the "first" (closest to the source command or bottom up in the tree) {@link InlineJoin}, replaces the {@link StubRelation}
     * of the right-hand side with left-hand side's source and returns a tuple.
     *
     * <p>After extracting the subplan, the method ensures that all {@link LocalRelation} nodes use {@link CopyingLocalSupplier}
     * to avoid double release when the same page is reused across executions. This guarantees correctness when substituting
     * the subplan result back into the main plan.
     *
     * <p>Example transformation:
     * <pre>
     * Original optimized plan:
     * Limit[1000[INTEGER],true]
     * \_InlineJoin[LEFT,[],[],[]]
     *   |_Limit[1000[INTEGER],false]
     *   | \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     *   \_Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     *     \_StubRelation[[x{r}#99]]
     *
     * Takes the right hand side:
     * Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     * \_StubRelation[[x{r}#99]]]
     *
     * And uses the left-hand side's source as its source:
     * Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     * \_Limit[1000[INTEGER],false]
     *   \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
     * </pre>
     *
     * <p>In the end the method returns a tuple like this:
     * <pre>
     * LogicalPlanTuple[
     *      stubReplacedSubPlan=
     *                          Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     *                          \_LocalRelation[[x{r}#99],org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier@7c1]
     *      originalSubPlan=
     *                          Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
     *                          \_StubRelation[[x{r}#99]]
     *                  ]
     * </pre>
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan, Set<LocalRelation> subPlansResults) {
        final Holder<LogicalPlan> stubReplacedSubPlanHolder = new Holder<>();
        final Holder<LogicalPlan> originalSubPlanHolder = new Holder<>();
        // Collect the first inlinejoin (bottom up in the tree)
        optimizedPlan.forEachUp(InlineJoin.class, ij -> {
            // extract the right side of the plan and replace its source
            if (stubReplacedSubPlanHolder.get() == null) {
                if (ij.right().anyMatch(p -> p instanceof StubRelation)) {
                    stubReplacedSubPlanHolder.set(replaceStub(ij.left(), ij.right()));
                } else if (ij.right() instanceof LocalRelation relation
                    && (subPlansResults.isEmpty() || subPlansResults.contains(relation) == false)
                    || ij.right() instanceof LocalRelation == false && ij.right().anyMatch(p -> p instanceof LocalRelation)) {
                        // In case the plan was optimized further and the StubRelation was replaced with a LocalRelation
                        // or the right hand side became a LocalRelation alltogether, there is no need to replace the source of the
                        // right-hand side anymore.
                        stubReplacedSubPlanHolder.set(ij.right());
                        // TODO: INLINE STATS this is essentially an optimization similar to the one in PruneInlineJoinOnEmptyRightSide
                        // this further supports the idea of running the optimization step again after the substitutions (see EsqlSession
                        // executeSubPlan() method where we could run the optimizer after the results are replaced in place).
                    }
                originalSubPlanHolder.set(ij.right());
            }
        });
        LogicalPlanTuple tuple = null;
        var plan = stubReplacedSubPlanHolder.get();
        if (plan != null) {
            plan = plan.transformUp(LocalRelation.class, lr -> {
                if (lr.supplier() instanceof CopyingLocalSupplier == false) {
                    return new LocalRelation(lr.source(), lr.output(), new CopyingLocalSupplier(lr.supplier().get()));
                }
                return lr;
            });
            plan.setOptimized();
            tuple = new LogicalPlanTuple(plan, originalSubPlanHolder.get());
        }
        return tuple;
    }

    public InlineJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
    }

    public InlineJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right, type, leftFields, rightFields, null);
    }

    private static InlineJoin readFrom(StreamInput in) throws IOException {
        PlanStreamInput planInput = (PlanStreamInput) in;
        Source source = Source.readFrom(planInput);
        LogicalPlan left = in.readNamedWriteable(LogicalPlan.class);
        LogicalPlan right = in.readNamedWriteable(LogicalPlan.class);
        JoinConfig config = new JoinConfig(in);
        // return new InlineJoin(source, left, replaceStub(left, right), config);
        return new InlineJoin(source, left, right, config);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Join> info() {
        // Do not just add the JoinConfig as a whole - this would prevent correctly registering the
        // expressions and references.
        JoinConfig config = config();
        return NodeInfo.create(this, InlineJoin::new, left(), right(), config.type(), config.leftFields(), config.rightFields());
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new InlineJoin(source(), left, right, config());
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        JoinType joinType = config().type();
        List<NamedExpression> output;
        if (LEFT.equals(joinType)) {
            List<? extends NamedExpression> leftOutputWithoutKeys = left.stream()
                .filter(ne -> config().leftFields().contains(ne.toAttribute()) == false)
                .toList();
            List<NamedExpression> rightWithAppendedLeftKeys = new ArrayList<>(right);
            rightWithAppendedLeftKeys.removeIf(ne -> config().rightFields().contains(ne.toAttribute()));
            AttributeMap<NamedExpression> leftAttrMap = AttributeMap.mapAll(left, NamedExpression::toAttribute);
            config().leftFields().forEach(lk -> rightWithAppendedLeftKeys.add(leftAttrMap.getOrDefault(lk, lk)));

            output = mergeOutputExpressions(rightWithAppendedLeftKeys, leftOutputWithoutKeys);
        } else {
            throw new IllegalArgumentException(joinType.joinName() + " unsupported");
        }
        return output;
    }
}
