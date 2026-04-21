/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.compute.aggregation.IntermediateStateDesc;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.approximation.ApproximationPlan;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * If the original aggregate wrapped by the sampled aggregate cannot be
 * pushed down to Lucene (which would execute exact and fast), sampling
 * should be used to speed up the aggregation.
 * <p>
 * In that case, this rule replaces the sampled aggregate by a regular
 * aggregate on top of a sample, with intermediate state corrections
 * for sample-corrected aggregates (COUNT, SUM). The plan:
 * <pre>
 * {@code FROM data | commands | SAMPLED_STATS[prob] aggs}
 * </pre>
 * is transformed into:
 * <pre>
 * {@code FROM data | SAMPLE prob | commands | STATS aggs | EVAL sample_correction}
 * </pre>
 */
public class ReplaceSampledStatsBySampleAndStats extends PhysicalOptimizerRules.OptimizerRule<SampledAggregateExec> {

    @Override
    protected PhysicalPlan rule(SampledAggregateExec plan) {
        double sampleProbability = (double) Foldables.literalValueOf(plan.sampleProbability());
        assert sampleProbability < 1.0;

        // The only non-unary plans that are currently supported are lookup joins.
        // At the moment, the left side of the join is the "expensive" side and
        // will be sampled, while the right side is just a lookup table.
        // This will probably change in the future, in which case this logic
        // must be reconsidered.
        assert plan.allMatch(p -> p instanceof LeafExec || p instanceof UnaryExec || p instanceof LookupJoinExec);

        Holder<Boolean> sampledAdded = new Holder<>(false);
        PhysicalPlan child = plan.child().transformDown(p -> {
            if (p instanceof LeafExec && sampledAdded.get() == false) {
                sampledAdded.set(true);
                return new SampleExec(Source.EMPTY, p, plan.sampleProbability());
            } else {
                return p;
            }
        });

        List<Alias> sampleCorrections = new ArrayList<>();
        List<Attribute> intermediateAttributes = new ArrayList<>();

        Expression bucketSampleProbability = new Div(
            Source.EMPTY,
            plan.sampleProbability(),
            Literal.integer(Source.EMPTY, ApproximationPlan.BUCKET_COUNT)
        );

        Set<String> originalIntermediateNames = plan.originalIntermediateAttributes()
            .stream()
            .map(NamedExpression::name)
            .collect(Collectors.toSet());

        // The first intermediate attributes are the grouping keys.
        int idx = 0;
        for (int g = 0; g < plan.groupings().size(); g++) {
            intermediateAttributes.add(plan.intermediateAttributes().get(idx++));
        }

        // The following intermediate attributes are the aggregates states.
        // They come in the same order as the aggregates.
        for (NamedExpression aggOrKey : plan.aggregates()) {
            if ((aggOrKey instanceof Alias alias && alias.child() instanceof AggregateFunction) == false) {
                // This is a grouping key and has already been added to the intermediate attributes.
                continue;
            }

            AggregateFunction aggFn = (AggregateFunction) ((Alias) aggOrKey).child();
            boolean aggFnNeedsCorrection = aggFn instanceof CountApproximate || aggFn instanceof Sum;

            List<IntermediateStateDesc> stateDescs = AggregateMapper.intermediateStateDesc(aggFn, plan.groupings().isEmpty() == false);
            for (IntermediateStateDesc desc : stateDescs) {
                Attribute attr = plan.intermediateAttributes().get(idx++);

                if (aggFnNeedsCorrection && desc.type() != ElementType.BOOLEAN) {
                    // Create a new alias for the uncorrected value, and reuse the existing attribute for the corrected value.
                    Alias uncorrectedAlias = new Alias(Source.EMPTY, attr.name(), attr);
                    intermediateAttributes.add(uncorrectedAlias.toAttribute());
                    Expression corrected = new Div(
                        Source.EMPTY,
                        uncorrectedAlias.toAttribute(),
                        originalIntermediateNames.contains(attr.name()) ? plan.sampleProbability() : bucketSampleProbability
                    );
                    Alias correctedAlias = new Alias(Source.EMPTY, attr.name(), corrected, attr.id());
                    sampleCorrections.add(correctedAlias);
                } else {
                    intermediateAttributes.add(attr);
                }
            }
        }

        PhysicalPlan result = new AggregateExec(
            plan.source(),
            child,
            plan.groupings(),
            plan.aggregates(),
            plan.getMode(),
            intermediateAttributes,
            plan.estimatedRowSize()
        );
        if (sampleCorrections.isEmpty() == false) {
            result = new ProjectExec(Source.EMPTY, new EvalExec(Source.EMPTY, result, sampleCorrections), plan.output());
        }
        return result;
    }
}
