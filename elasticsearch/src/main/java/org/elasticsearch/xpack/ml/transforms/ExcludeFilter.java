/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.xpack.ml.job.config.Condition;


/**
 * Abstract base class for exclude filters
 */
public abstract class ExcludeFilter extends Transform {
    private final Condition condition;

    /**
     * The condition should have been verified by now and it <i>must</i> have a
     * valid value &amp; operator
     */
    public ExcludeFilter(Condition condition, List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        super(readIndexes, writeIndexes, logger);
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }
}
