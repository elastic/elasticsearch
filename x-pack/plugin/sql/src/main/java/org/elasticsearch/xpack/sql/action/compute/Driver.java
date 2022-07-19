/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Driver implements Runnable {

    private final List<Operator> activeOperators;
    private final Releasable releasable;

    public Driver(List<Operator> operators, Releasable releasable) {
        this.activeOperators = new ArrayList<>(operators);
        this.releasable = releasable;
    }

    public void run() {
        while (isFinished() == false) {
            runLoopIteration();
        }
        releasable.close();
    }

    public ListenableActionFuture<Void> run(TimeValue maxTime, int maxIterations) {
        long maxTimeNanos = maxTime.nanos();
        long startTime = System.nanoTime();
        int iter = 0;
        while (isFinished() == false) {
            ListenableActionFuture<Void> fut = runLoopIteration();
            if (fut.isDone() == false) {
                return fut;
            }
            if (++iter >= maxIterations) {
                break;
            }
            long now = System.nanoTime();
            if (now - startTime > maxTimeNanos) {
                break;
            }
        }
        if (isFinished()) {
            releasable.close();
        }
        return Operator.NOT_BLOCKED;
    }

    public boolean isFinished() {
        return activeOperators.isEmpty();
    }

    private ListenableActionFuture<Void> runLoopIteration() {

        boolean movedPage = false;

        for (int i = 0; i < activeOperators.size() - 1; i++) {
            Operator op = activeOperators.get(i);
            Operator nextOp = activeOperators.get(i + 1);

            // skip blocked operator
            if (op.isBlocked().isDone() == false) {
                continue;
            }

            if (op.isFinished() == false && nextOp.isBlocked().isDone() && nextOp.needsInput()) {
                Page page = op.getOutput();
                if (page != null && page.getPositionCount() != 0) {
                    nextOp.addInput(page);
                    movedPage = true;
                }

                if (op instanceof SourceOperator) {
                    movedPage = true;
                }
            }

            if (op.isFinished()) {
                nextOp.finish();
            }
        }

        for (int index = activeOperators.size() - 1; index >= 0; index--) {
            if (activeOperators.get(index).isFinished()) {
                // close and remove this operator and all source operators
                List<Operator> finishedOperators = this.activeOperators.subList(0, index + 1);
                finishedOperators.stream().forEach(Operator::close);
                finishedOperators.clear();

                // Finish the next operator, which is now the first operator.
                if (activeOperators.isEmpty() == false) {
                    Operator newRootOperator = activeOperators.get(0);
                    newRootOperator.finish();
                }
                break;
            }
        }

        if (movedPage == false) {
            return oneOf(activeOperators.stream()
                .map(Operator::isBlocked)
                .filter(laf -> laf.isDone() == false)
                .collect(Collectors.toList()));
        }
        return Operator.NOT_BLOCKED;
    }

    private static ListenableActionFuture<Void> oneOf(List<ListenableActionFuture<Void>> futures) {
        if (futures.isEmpty()) {
            return Operator.NOT_BLOCKED;
        }
        if (futures.size() == 1) {
            return futures.get(0);
        }
        ListenableActionFuture<Void> oneOf = new ListenableActionFuture<>();
        for (ListenableActionFuture<Void> fut : futures) {
            fut.addListener(oneOf);
        }
        return oneOf;
    }
}
