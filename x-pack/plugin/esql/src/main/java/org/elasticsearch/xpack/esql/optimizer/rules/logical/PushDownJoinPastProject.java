/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Pushing down {@link Join}s past {@link Project}s has the benefit that field extraction can happen later. Also, once bubbled downstream,
 * multiple projects can be combined which can eliminate some fields altogether (c.f.
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction}). Even just field extractions before joins are
 * expensive because joins create new rows in case of multiple matches, which means that the extracted columns need to be deeply copied
 * and blown up.
 *
 * This follows the same approach as {@link PushDownUtils#pushGeneratingPlanPastProjectAndOrderBy(UnaryPlan)}. To deal with name conflicts,
 * we rename the fields that a {@code LOOKUP JOIN} "generates" by renaming the {@link FieldAttribute}s in the join's right hand side
 * {@link EsRelation}. Once we have qualifiers, this can be simplified by just assigning temporary qualifiers and later stripping them away.
 */
public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {

    /**
     * Wrapper class for a {@link Join} representing a {@code LOOKUP JOIN}, so we can treat it as if it was a {@link UnaryPlan} that is also
     * a {@link GeneratingPlan};
     */
    private class JoinAsUnaryGeneratingPlan extends UnaryPlan implements GeneratingPlan<JoinAsUnaryGeneratingPlan> {
        private final Join lookupJoin;
        private List<Attribute> lazyGeneratedAttributes;

        JoinAsUnaryGeneratingPlan(Join lookupJoin) {
            super(lookupJoin.source(), lookupJoin.left());
            this.lookupJoin = lookupJoin;
        }

        private JoinAsUnaryGeneratingPlan(
            Source source,
            LogicalPlan left,
            LogicalPlan right,
            JoinType type,
            List<Attribute> matchFields,
            List<Attribute> leftFields,
            List<Attribute> rightFields
        ) {
            this(new Join(source, left, right, type, matchFields, leftFields, rightFields));
        }

        Join unwrap() {
            return lookupJoin;
        }

        @Override
        public UnaryPlan replaceChild(LogicalPlan newChild) {
            return new JoinAsUnaryGeneratingPlan(lookupJoin.replaceChildren(newChild, lookupJoin.right()));
        }

        @Override
        public boolean expressionsResolved() {
            return lookupJoin.expressionsResolved();
        }

        @Override
        protected NodeInfo<? extends LogicalPlan> info() {
            return NodeInfo.create(
                this,
                JoinAsUnaryGeneratingPlan::new,
                lookupJoin.left(),
                lookupJoin.right(),
                lookupJoin.config().type(),
                lookupJoin.config().matchFields(),
                lookupJoin.config().leftFields(),
                lookupJoin.config().rightFields()
            );
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("lives only for a single optimizer rule application");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("lives only for a single optimizer rule application");
        }

        @Override
        public List<Attribute> generatedAttributes() {
            if (lazyGeneratedAttributes == null) {
                lazyGeneratedAttributes = lookupJoin.rightOutputFields();
            }
            return lazyGeneratedAttributes;
        }

        @Override
        public JoinAsUnaryGeneratingPlan withGeneratedNames(List<String> newNames) {
            checkNumberOfNewNames(newNames);

            if (lookupJoin.right() instanceof EsRelation esRelation) {
                AttributeSet generatedSet = AttributeSet.of(generatedAttributes());
                int numOutputAttributes = esRelation.output().size();
                List<Attribute> newAttributes = new ArrayList<>(numOutputAttributes);
                // The match field from the LOOKUP JOIN's right hand side EsRelation is not added to the output from the left hand side.
                // It's not part of the "generated" attributes and needs to be skipped.
                int newNamesIndex = 0;
                for (Attribute attr : esRelation.output()) {
                    if (generatedSet.contains(attr)) {
                        String newName = newNames.get(newNamesIndex++);
                        if (newName.equals(attr.name())) {
                            newAttributes.add(attr);
                        } else {
                            newAttributes.add(attr.withName(newName).withId(new NameId()));
                        }
                    } else {
                        newAttributes.add(attr);
                    }
                }

                assert newAttributes.size() == numOutputAttributes;
                return new JoinAsUnaryGeneratingPlan(
                    lookupJoin.replaceChildren(lookupJoin.left(), esRelation.withAttributes(newAttributes))
                );
            }
            throw new IllegalStateException(
                "right hand side of LOOKUP JOIN must be a relation, found [" + lookupJoin.right().getClass() + "]"
            );
        }

        @Override
        public int hashCode() {
            return lookupJoin.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JoinAsUnaryGeneratingPlan other = (JoinAsUnaryGeneratingPlan) obj;

            return lookupJoin.equals(other.lookupJoin);
        }
    }

    @Override
    protected LogicalPlan rule(Join join) {
        if (join.left() instanceof Project projectChild
            && JoinTypes.LEFT.equals(join.config().type())
            && join.right() instanceof EsRelation lookupIndex
            && lookupIndex.indexMode() == IndexMode.LOOKUP) {

            var joinAsGeneratingUnary = new JoinAsUnaryGeneratingPlan(join);
            var pushedDown = PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(joinAsGeneratingUnary);

            return pushedDown.transformDown(JoinAsUnaryGeneratingPlan.class, JoinAsUnaryGeneratingPlan::unwrap);
        }

        return join;

    }
}
