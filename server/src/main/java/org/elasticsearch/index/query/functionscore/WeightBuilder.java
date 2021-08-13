/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

/**
 * A query that multiplies the weight to the score.
 */
public class WeightBuilder extends ScoreFunctionBuilder<WeightBuilder> {
    public static final String NAME = "weight";

    /**
     * Standard constructor.
     */
    public WeightBuilder() {
    }

    /**
     * Read from a stream.
     */
    public WeightBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    }

    @Override
    protected boolean doEquals(WeightBuilder functionBuilder) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    protected ScoreFunction doToFunction(SearchExecutionContext context) throws IOException {
        //nothing to do here, weight will be applied by the parent class, no score function
        return null;
    }
}
