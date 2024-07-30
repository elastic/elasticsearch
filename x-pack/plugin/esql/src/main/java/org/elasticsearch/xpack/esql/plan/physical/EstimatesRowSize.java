/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

public interface EstimatesRowSize {
    static PhysicalPlan estimateRowSize(int extraRowSize, PhysicalPlan plan) {
        EstimatesRowSize.State state = new EstimatesRowSize.State();
        state.maxEstimatedRowSize = state.estimatedRowSize = extraRowSize;
        return plan.transformDown(exec -> {
            if (exec instanceof EstimatesRowSize r) {
                return r.estimateRowSize(state);
            }
            return exec;
        });
    }

    /**
     * Estimate the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     * @return
     */
    PhysicalPlan estimateRowSize(State state);

    final class State {
        /**
         * Estimated size of rows added by later operations.
         */
        private int estimatedRowSize;

        /**
         * Max value that {@link #estimatedRowSize} has had since the last
         * call to {@link #consumeAllFields}.
         */
        private int maxEstimatedRowSize;

        /**
         * True if there is an operation that needs a sorted list of
         * document ids (like {@link FieldExtractExec}) before the node
         * being visited. That's used to add more bytes to any operation
         * that loads documents out of order.
         */
        private boolean needsSortedDocIds;

        /**
         * Model an operator that has a fixed overhead.
         */
        public void add(boolean needsSortedDocIds, int bytes) {
            estimatedRowSize += bytes;
            maxEstimatedRowSize = Math.max(estimatedRowSize, maxEstimatedRowSize);
            this.needsSortedDocIds |= needsSortedDocIds;
        }

        /**
         * Model an operator that adds fields.
         */
        public void add(boolean needsSortedDocIds, List<? extends Expression> expressions) {
            expressions.stream().forEach(a -> estimatedRowSize += estimateSize(a.dataType()));
            maxEstimatedRowSize = Math.max(estimatedRowSize, maxEstimatedRowSize);
            this.needsSortedDocIds |= needsSortedDocIds;
        }

        /**
         * Model an operator that consumes all fields.
         * @return the number of bytes added to pages emitted by the operator
         *         being modeled
         */
        public int consumeAllFields(boolean producesUnsortedDocIds) {
            int size = maxEstimatedRowSize;
            if (producesUnsortedDocIds && needsSortedDocIds) {
                size += DocVector.SHARD_SEGMENT_DOC_MAP_PER_ROW_OVERHEAD;
            }
            estimatedRowSize = maxEstimatedRowSize = 0;
            needsSortedDocIds = false;
            return size;
        }

        @Override
        public String toString() {
            return "State{"
                + "estimatedRowSize="
                + estimatedRowSize
                + ", maxEstimatedRowSize="
                + maxEstimatedRowSize
                + ", needsSortedDocIds="
                + needsSortedDocIds
                + '}';
        }
    }

    static int estimateSize(DataType dataType) {
        ElementType elementType = PlannerUtils.toElementType(dataType);
        if (elementType == ElementType.DOC) {
            throw new EsqlIllegalArgumentException("can't load a [doc] with field extraction");
        }
        if (elementType == ElementType.UNKNOWN) {
            throw new EsqlIllegalArgumentException("[unknown] can't be the result of field extraction");
        }
        return dataType.estimatedSize().orElse(50);
    }
}
