/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.execution.search.extractor.ScoreExtractor;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.HitExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

import static java.util.Collections.emptyList;

public class ScorePipe extends Pipe {
    public ScorePipe(Source source, Expression expression) {
        super(source, expression, emptyList());
    }

    @Override
    protected NodeInfo<ScorePipe> info() {
        return NodeInfo.create(this, ScorePipe::new, expression());
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public Processor asProcessor() {
        return new HitExtractorProcessor(ScoreExtractor.INSTANCE);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return false;
    }

    @Override
    public Pipe resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        sourceBuilder.trackScores();
    }
}
