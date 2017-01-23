/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.Operator;


/**
 * Parses a numeric value from a field and compares it against a hard
 * value using a certain {@link Operator}
 */
public class ExcludeFilterNumeric extends ExcludeFilter {
    private final double filterValue;

    /**
     * The condition should have been verified by now but if they are not valid
     * then the default of &lt; (less than) and filter of 0.0 are used meaning
     * that no values are excluded.
     */
    public ExcludeFilterNumeric(Condition condition, List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        super(condition, readIndexes, writeIndexes, logger);

        filterValue = parseFilterValue(getCondition().getValue());
    }

    /**
     * If no condition then the default is &lt; (less than) and filter value of
     * 0.0 are used meaning that only -ve values are excluded.
     */
    public ExcludeFilterNumeric(List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        super(new Condition(Operator.LT, "0.0"),
                readIndexes, writeIndexes, logger);
        filterValue = 0.0;
    }

    private double parseFilterValue(String fieldValue) {
        double result = 0.0;
        try {
            result = Double.parseDouble(fieldValue);
        } catch (NumberFormatException e) {
            logger.warn("Exclude transform cannot parse a number from field '" + fieldValue + "'. Using default 0.0");
        }

        return result;
    }

    /**
     * Returns {@link TransformResult#EXCLUDE} if the value should be excluded
     */
    @Override
    public TransformResult transform(String[][] readWriteArea)
            throws TransformException {
        TransformResult result = TransformResult.OK;
        for (TransformIndex readIndex : readIndexes) {
            String field = readWriteArea[readIndex.array][readIndex.index];

            try {
                double value = Double.parseDouble(field);

                if (getCondition().getOperator().test(value, filterValue)) {
                    result = TransformResult.EXCLUDE;
                    break;
                }
            } catch (NumberFormatException e) {

            }
        }

        return result;
    }

    public double filterValue() {
        return filterValue;
    }
}
