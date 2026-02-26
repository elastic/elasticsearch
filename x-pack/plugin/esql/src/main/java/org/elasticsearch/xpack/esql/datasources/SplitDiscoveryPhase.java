/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;

/**
 * Walks the physical plan tree, discovers splits for each {@link ExternalSourceExec},
 * and replaces them with split-enriched copies via {@link ExternalSourceExec#withSplits}.
 *
 * <p>Filter expressions from {@link FilterExec} ancestors are collected per-source so that
 * each {@link ExternalSourceExec} only receives filters from its own ancestor chain, not
 * from unrelated branches of the plan tree.
 */
public final class SplitDiscoveryPhase {

    private SplitDiscoveryPhase() {}

    public static PhysicalPlan resolveExternalSplits(PhysicalPlan plan, Map<String, ExternalSourceFactory> sourceFactories) {
        return resolveRecursive(plan, List.of(), sourceFactories);
    }

    private static PhysicalPlan resolveRecursive(
        PhysicalPlan plan,
        List<Expression> ancestorFilters,
        Map<String, ExternalSourceFactory> sourceFactories
    ) {
        if (plan instanceof ExternalSourceExec exec) {
            return resolveExternalSource(exec, ancestorFilters, sourceFactories);
        }

        List<Expression> filtersForChildren = ancestorFilters;
        if (plan instanceof FilterExec filterExec) {
            List<Expression> extended = new ArrayList<>(ancestorFilters);
            for (Expression conjunction : splitAnd(filterExec.condition())) {
                extended.add(conjunction);
            }
            filtersForChildren = List.copyOf(extended);
        }

        List<PhysicalPlan> children = plan.children();
        if (children.isEmpty()) {
            return plan;
        }

        boolean changed = false;
        List<PhysicalPlan> newChildren = new ArrayList<>(children.size());
        for (PhysicalPlan child : children) {
            PhysicalPlan resolved = resolveRecursive(child, filtersForChildren, sourceFactories);
            if (resolved != child) {
                changed = true;
            }
            newChildren.add(resolved);
        }

        if (changed == false) {
            return plan;
        }

        if (plan instanceof UnaryExec unary && newChildren.size() == 1) {
            return unary.replaceChild(newChildren.get(0));
        }
        return plan.replaceChildren(newChildren);
    }

    private static PhysicalPlan resolveExternalSource(
        ExternalSourceExec exec,
        List<Expression> ancestorFilters,
        Map<String, ExternalSourceFactory> sourceFactories
    ) {
        ExternalSourceFactory factory = sourceFactories.get(exec.sourceType());
        SplitProvider splitProvider = factory != null ? factory.splitProvider() : SplitProvider.SINGLE;

        FileSet fileSet = exec.fileSet();
        PartitionMetadata partitionInfo = fileSet != null ? fileSet.partitionMetadata() : null;

        SplitDiscoveryContext context = new SplitDiscoveryContext(
            null,
            fileSet != null ? fileSet : FileSet.UNRESOLVED,
            exec.config(),
            partitionInfo,
            ancestorFilters
        );

        List<ExternalSplit> splits = splitProvider.discoverSplits(context);
        if (splits.isEmpty()) {
            return exec;
        }
        return exec.withSplits(splits);
    }
}
