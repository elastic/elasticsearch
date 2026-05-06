/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Approximation for a query that contains {@code FORK} with one {@link Approximation} per branch.
 * <p>
 * The source count is shared across all branches (run once), and the filtered count subplans
 * for all unconverged branches are combined into a single FORK plan so they execute in parallel.
 * Each branch is independently calibrated via its own sample probability.
 */
public final class ForkApproximation implements ApproximationDriver {

    private final List<Approximation> branches;
    private boolean sourceCountDone;

    ForkApproximation(LogicalPlan logicalPlan, ApproximationSettings settings) {
        List<Fork> forks = new ArrayList<>();
        logicalPlan.forEachUp(Fork.class, forks::add);
        Fork fork = forks.getFirst();
        branches = new ArrayList<>();
        for (LogicalPlan child : fork.children()) {
            try {
                branches.add(new Approximation(child, settings));
            } catch (VerificationException e) {
                branches.add(null);
            }
        }
        sourceCountDone = false;
    }

    @Override
    public LogicalPlan firstSubPlan() {
        if (sourceCountDone == false) {
            return branches.stream().filter(Objects::nonNull).findFirst().get().firstSubPlan();
        } else {
            return countSubPlan();
        }
    }

    @Override
    public LogicalPlan newMainPlan(LogicalPlan mainPlan, Result result) {
        if (sourceCountDone == false) {
            return processSourceCount(result, mainPlan);
        } else {
            return processCount(result, mainPlan);
        }
    }

    /**
     * Processes the shared source count result, feeding it to every approximable branch.
     * Branches that converge immediately (e.g. row-preserving) get their probability substituted.
     */
    private LogicalPlan processSourceCount(Result result, LogicalPlan mainPlan) {
        sourceCountDone = true;
        long rowCount = Approximation.rowCount(result);
        for (int branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
            Approximation branch = branches.get(branchIndex);
            if (branch != null) {
                Double sampleProbability = branch.processResult(rowCount);
                if (sampleProbability != null) {
                    mainPlan = substituteSampleProbability(mainPlan, branchIndex, sampleProbability);
                }
            }
        }
        return mainPlan;
    }

    /**
     * Builds a FORK plan that combines the count subplans of all unconverged branches.
     * Each branch gets an {@code EVAL _fork = "fork{i}"} so the result rows can be distinguished.
     * Returns {@code null} when all branches have converged.
     */
    private LogicalPlan countSubPlan() {
        List<LogicalPlan> countBranches = new ArrayList<>();
        for (int i = 0; i < branches.size(); i++) {
            Approximation branch = branches.get(i);
            if (branch != null) {
                LogicalPlan countPlan = branch.firstSubPlan();
                if (countPlan != null) {
                    Literal forkLabel = Literal.integer(Source.EMPTY, i);
                    countBranches.add(new Eval(Source.EMPTY, countPlan, List.of(new Alias(Source.EMPTY, Fork.FORK_FIELD, forkLabel))));
                }
            }
        }
        if (countBranches.isEmpty()) {
            return null;
        }
        Fork forkPlan = new Fork(Source.EMPTY, countBranches, Fork.outputUnion(countBranches));
        forkPlan.setOptimized();
        return forkPlan;
    }

    /**
     * Processes the multi-row FORK count result. Each row's branch is identified by the
     * {@code _fork} column (e.g. {@code "fork0"}, {@code "fork1"}) rather than by row position,
     * because MergeExec does not guarantee child-order delivery.
     */
    private LogicalPlan processCount(Result result, LogicalPlan mainPlan) {
        for (Page page : result.pages()) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                int branchIndex = ((IntBlock) page.getBlock(1)).getInt(position);
                long rowCount = switch (page.getBlock(0)) {
                    case DoubleBlock doubleBlock -> Math.round(doubleBlock.getDouble(position));
                    case LongBlock longBlock -> longBlock.getLong(position);
                    default -> throw new IllegalStateException("Unexpected block type: " + page.getBlock(0));
                };
                Double p = branches.get(branchIndex).processCount(rowCount);
                if (p != null) {
                    mainPlan = substituteSampleProbability(mainPlan, branchIndex, p);
                }
            }
            page.close();
        }
        return mainPlan;
    }

    private LogicalPlan substituteSampleProbability(LogicalPlan mainPlan, int branchIndex, double sampleProbability) {
        mainPlan = mainPlan.transformUp(Fork.class, fork -> {
            List<LogicalPlan> children = new ArrayList<>(fork.children());
            assert branchIndex >= 0 && branchIndex < children.size();
            children.set(branchIndex, ApproximationPlan.substituteSampleProbabilityInForkBranch(children.get(branchIndex), sampleProbability));
            return fork.replaceSubPlans(children).refreshOutput();
        });
        mainPlan.setOptimized();
        return mainPlan;
    }
}
