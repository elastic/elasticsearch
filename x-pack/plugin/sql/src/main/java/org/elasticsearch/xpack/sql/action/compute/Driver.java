/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;

public class Driver implements Runnable {

    private final List<Operator> activeOperators;
    private final Releasable releasable;

    public Driver(List<Operator> operators, Releasable releasable) {
        this.activeOperators = new ArrayList<>(operators);
        this.releasable = releasable;
    }

    public void run() {
        while (activeOperators.isEmpty() == false) {
            runLoopIteration();
        }
        releasable.close();
    }

    private void runLoopIteration() {
        for (int i = 0; i < activeOperators.size() - 1; i++) {
            Operator op = activeOperators.get(i);
            Operator nextOp = activeOperators.get(i + 1);

            if (op.isFinished() == false && nextOp.needsInput()) {
                Page page = op.getOutput();
                if (page != null) {
                    nextOp.addInput(page);
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
    }
}
