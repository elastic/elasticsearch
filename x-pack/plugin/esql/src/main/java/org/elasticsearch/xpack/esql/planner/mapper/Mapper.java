/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampleExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plan.physical.inference.RerankExec;

import java.util.List;

/**
 * <p>This class is part of the planner</p>
 *
 * <p>Translates the logical plan into a physical plan.  This is where we start to decide what will be executed on the data nodes and what
 * will be executed on the coordinator nodes.  This step creates {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec} instances,
 * which represent logical plan fragments to be sent to the data nodes and {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeExec}
 * instances, which represent data being sent back from the data nodes to the coordinating node.</p>
 */
public class Mapper {

    public PhysicalPlan map(LogicalPlan p) {

        if (p instanceof LeafPlan leaf) {
            return mapLeaf(leaf);
        }

        if (p instanceof UnaryPlan unary) {
            return mapUnary(unary);
        }

        if (p instanceof BinaryPlan binary) {
            return mapBinary(binary);
        }

        if (p instanceof Fork fork) {
            return mapFork(fork);
        }

        return MapperUtils.unsupported(p);
    }

    private PhysicalPlan mapLeaf(LeafPlan leaf) {
        if (leaf instanceof EsRelation esRelation) {
            return new FragmentExec(esRelation);
        }

        return MapperUtils.mapLeaf(leaf);
    }

    private PhysicalPlan mapUnary(UnaryPlan unary) {
        PhysicalPlan mappedChild = map(unary.child());

        //
        // TODO - this is hard to follow and needs reworking
        // https://github.com/elastic/elasticsearch/issues/115897
        //
        if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.REMOTE) {
            // When we have remote enrich, we want to put it under FragmentExec, so it would be executed remotely.
            // We're only going to do it on the coordinator node.
            // The way we're going to do it is as follows:
            // 1. Locate FragmentExec in the tree. If we have no FragmentExec, we won't do anything.
            // 2. Put this Enrich under it, removing everything that was below it previously.
            // 3. Above FragmentExec, we should deal with pipeline breakers, since pipeline ops already are supposed to go under
            // FragmentExec.
            // 4. Aggregates can't appear here since the plan should have errored out if we have aggregate inside remote Enrich.
            // 5. So we should be keeping: LimitExec, ExchangeExec, OrderExec, TopNExec (actually OrderExec probably can't happen anyway).
            Holder<Boolean> hasFragment = new Holder<>(false);

            var childTransformed = mappedChild.transformUp(f -> {
                // Once we reached FragmentExec, we stuff our Enrich under it
                if (f instanceof FragmentExec) {
                    hasFragment.set(true);
                    return new FragmentExec(enrich);
                }
                if (f instanceof EnrichExec enrichExec) {
                    // It can only be ANY because COORDINATOR would have errored out earlier, and REMOTE should be under FragmentExec
                    assert enrichExec.mode() == Enrich.Mode.ANY : "enrich must be in ANY mode here";
                    return enrichExec.child();
                }
                if (f instanceof UnaryExec unaryExec) {
                    if (f instanceof LimitExec || f instanceof ExchangeExec || f instanceof TopNExec) {
                        return f;
                    } else {
                        return unaryExec.child();
                    }
                }
                // Currently, it's either UnaryExec or LeafExec. Leaf will either resolve to FragmentExec or we'll ignore it.
                return f;
            });

            if (hasFragment.get()) {
                return childTransformed;
            }
        }

        if (mappedChild instanceof FragmentExec) {
            // COORDINATOR enrich must not be included to the fragment as it has to be executed on the coordinating node
            if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR) {
                mappedChild = addExchangeForFragment(enrich.child(), mappedChild);
                return MapperUtils.mapUnary(unary, mappedChild);
            }
            // in case of a fragment, push to it any current streaming operator
            if (isPipelineBreaker(unary) == false) {
                return new FragmentExec(unary);
            }
        }

        //
        // Pipeline breakers
        //
        if (unary instanceof Aggregate aggregate) {
            List<Attribute> intermediate = MapperUtils.intermediateAttributes(aggregate);

            // create both sides of the aggregate (for parallelism purposes), if no fragment is present
            // TODO: might be easier long term to end up with just one node and split if necessary instead of doing that always at this
            // stage
            mappedChild = addExchangeForFragment(aggregate, mappedChild);

            // exchange was added - use the intermediates for the output
            if (mappedChild instanceof ExchangeExec exchange) {
                mappedChild = new ExchangeExec(mappedChild.source(), intermediate, true, exchange.child());
            }
            // if no exchange was added (aggregation happening on the coordinator), create the initial agg
            else {
                mappedChild = MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.INITIAL, intermediate);
            }

            // always add the final/reduction agg
            return MapperUtils.aggExec(aggregate, mappedChild, AggregatorMode.FINAL, intermediate);
        }

        if (unary instanceof Limit limit) {
            mappedChild = addExchangeForFragment(limit, mappedChild);
            return new LimitExec(limit.source(), mappedChild, limit.limit());
        }

        if (unary instanceof TopN topN) {
            mappedChild = addExchangeForFragment(topN, mappedChild);
            return new TopNExec(topN.source(), mappedChild, topN.order(), topN.limit(), null);
        }

        if (unary instanceof Rerank rerank) {
            mappedChild = addExchangeForFragment(rerank, mappedChild);
            return new RerankExec(
                rerank.source(),
                mappedChild,
                rerank.inferenceId(),
                rerank.queryText(),
                rerank.rerankFields(),
                rerank.scoreAttribute()
            );
        }

        // TODO: share code with local LocalMapper?
        if (unary instanceof Sample sample) {
            mappedChild = addExchangeForFragment(sample, mappedChild);
            return new SampleExec(sample.source(), mappedChild, sample.probability(), sample.seed());
        }

        //
        // Pipeline operators
        //
        return MapperUtils.mapUnary(unary, mappedChild);
    }

    private PhysicalPlan mapBinary(BinaryPlan bp) {
        if (bp instanceof Join join) {
            JoinConfig config = join.config();
            if (config.type() != JoinTypes.LEFT) {
                throw new EsqlIllegalArgumentException("unsupported join type [" + config.type() + "]");
            }

            if (join instanceof InlineJoin) {
                return new FragmentExec(bp);
            }

            PhysicalPlan left = map(bp.left());

            // only broadcast joins supported for now - hence push down as a streaming operator
            if (left instanceof FragmentExec fragment) {
                return new FragmentExec(bp);
            }

            PhysicalPlan right = map(bp.right());
            // if the right is data we can use a hash join directly
            if (right instanceof LocalSourceExec localData) {
                return new HashJoinExec(
                    join.source(),
                    left,
                    localData,
                    config.matchFields(),
                    config.leftFields(),
                    config.rightFields(),
                    join.output()
                );
            }
            if (right instanceof FragmentExec fragment
                && fragment.fragment() instanceof EsRelation relation
                && relation.indexMode() == IndexMode.LOOKUP) {
                return new LookupJoinExec(join.source(), left, right, config.leftFields(), config.rightFields(), join.rightOutputFields());
            }
        }

        return MapperUtils.unsupported(bp);
    }

    private PhysicalPlan mapFork(Fork fork) {
        return new MergeExec(fork.source(), fork.children().stream().map(child -> map(child)).toList(), fork.output());
    }

    public static boolean isPipelineBreaker(LogicalPlan p) {
        return p instanceof Aggregate || p instanceof TopN || p instanceof Limit || p instanceof OrderBy;
    }

    private PhysicalPlan addExchangeForFragment(LogicalPlan logical, PhysicalPlan child) {
        // in case of fragment, preserve the streaming operator (order-by, limit or topN) for local replanning
        // no need to do it for an aggregate since it gets split
        // and clone it as a physical node along with the exchange
        if (child instanceof FragmentExec) {
            child = new FragmentExec(logical);
            child = new ExchangeExec(child.source(), child);
        }
        return child;
    }
}
