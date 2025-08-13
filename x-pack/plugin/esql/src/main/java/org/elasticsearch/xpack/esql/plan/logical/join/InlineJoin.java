/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;
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
public class InlineJoin extends Join {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "InlineJoin",
        InlineJoin::readFrom
    );

    /**
     * Replaces the source of the target plan with a stub preserving the output of the source plan.
     */
    public static LogicalPlan stubSource(UnaryPlan sourcePlan, LogicalPlan target) {
        return sourcePlan.replaceChild(new StubRelation(sourcePlan.source(), target.output()));
    }

    /**
     * TODO: perform better planning
     * Keep the join in place or replace it with an Eval in case no grouping is necessary.
     */
    public static LogicalPlan inlineData(InlineJoin target, LocalRelation data) {
        if (target.config().matchFields().isEmpty()) {
            List<Attribute> schema = data.output();
            Block[] blocks = data.supplier().get();
            List<Alias> aliases = new ArrayList<>(schema.size());
            for (int i = 0; i < schema.size(); i++) {
                Attribute attr = schema.get(i);
                aliases.add(new Alias(attr.source(), attr.name(), Literal.of(attr, BlockUtils.toJavaObject(blocks[i], 0)), attr.id()));
            }
            return new Eval(target.source(), target.left(), aliases);
        } else {
            return target.replaceRight(data);
        }
    }

    /**
     * Replaces the stubbed source with the actual source.
     * NOTE: this will replace all {@link StubRelation}s found with the source and the method is meant to be used to replace one node only
     * when being called on a plan that has only ONE StubRelation in it.
     */
    public static LogicalPlan replaceStub(LogicalPlan source, LogicalPlan stubbed) {
        // here we could have used stubbed.transformUp(StubRelation.class, stubRelation -> source)
        // but transformUp skips changing a node if its transformed variant is equal to its original variant.
        // A StubRelation can contain in its output ReferenceAttributes which do not use NameIds for equality, but only names and
        // two ReferenceAttributes with the same name are equal and the transformation will not be applied.
        return stubbed.transformUp(UnaryPlan.class, up -> {
            if (up.child() instanceof StubRelation) {
                return up.replaceChild(source);
            }
            return up;
        });
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
     */
    public static LogicalPlanTuple firstSubPlan(LogicalPlan optimizedPlan) {
        Holder<LogicalPlanTuple> subPlan = new Holder<>();
        // Collect the first inlinejoin (bottom up in the tree)
        optimizedPlan.forEachUp(InlineJoin.class, ij -> {
            // extract the right side of the plan and replace its source
            if (subPlan.get() == null && ij.right().anyMatch(p -> p instanceof StubRelation)) {
                var p = replaceStub(ij.left(), ij.right());
                p.setOptimized();
                subPlan.set(new LogicalPlanTuple(p, ij.right()));
            }
        });
        return subPlan.get();
    }

    public InlineJoin(Source source, LogicalPlan left, LogicalPlan right, JoinConfig config) {
        super(source, left, right, config);
    }

    public InlineJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        JoinType type,
        List<Attribute> matchFields,
        List<Attribute> leftFields,
        List<Attribute> rightFields
    ) {
        super(source, left, right, type, matchFields, leftFields, rightFields);
    }

    private static InlineJoin readFrom(StreamInput in) throws IOException {
        PlanStreamInput planInput = (PlanStreamInput) in;
        Source source = Source.readFrom(planInput);
        LogicalPlan left = in.readNamedWriteable(LogicalPlan.class);
        LogicalPlan right = in.readNamedWriteable(LogicalPlan.class);
        JoinConfig config = new JoinConfig(in);
        return new InlineJoin(source, left, replaceStub(left, right), config);
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
        return NodeInfo.create(
            this,
            InlineJoin::new,
            left(),
            right(),
            config.type(),
            config.matchFields(),
            config.leftFields(),
            config.rightFields()
        );
    }

    @Override
    public Join replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new InlineJoin(source(), left, right, config());
    }

    @Override
    public List<Attribute> computeOutput(List<Attribute> left, List<Attribute> right) {
        JoinType joinType = config().type();
        List<Attribute> output;
        if (LEFT.equals(joinType)) {
            List<Attribute> leftOutputWithoutKeys = left.stream().filter(attr -> config().leftFields().contains(attr) == false).toList();
            List<Attribute> rightWithAppendedKeys = new ArrayList<>(right);
            rightWithAppendedKeys.removeAll(config().rightFields());
            rightWithAppendedKeys.addAll(config().leftFields());

            output = mergeOutputAttributes(rightWithAppendedKeys, leftOutputWithoutKeys);
        } else {
            throw new IllegalArgumentException(joinType.joinName() + " unsupported");
        }
        return output;
    }
}
