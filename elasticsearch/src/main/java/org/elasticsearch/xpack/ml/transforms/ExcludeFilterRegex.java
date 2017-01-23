/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.elasticsearch.xpack.ml.job.config.Condition;

/**
 * Matches a field against a regex
 */
public class ExcludeFilterRegex extends ExcludeFilter {
    private final Pattern pattern;

    public ExcludeFilterRegex(Condition condition, List<TransformIndex> readIndexes,
            List<TransformIndex> writeIndexes, Logger logger) {
        super(condition, readIndexes, writeIndexes, logger);

        pattern = Pattern.compile(getCondition().getValue());
    }

    /**
     * Returns {@link TransformResult#EXCLUDE} if the record matches the regex
     */
    @Override
    public TransformResult transform(String[][] readWriteArea)
            throws TransformException {
        TransformResult result = TransformResult.OK;
        for (TransformIndex readIndex : readIndexes) {
            String field = readWriteArea[readIndex.array][readIndex.index];
            Matcher match = pattern.matcher(field);

            if (match.matches()) {
                result = TransformResult.EXCLUDE;
                break;
            }
        }

        return result;
    }

}
