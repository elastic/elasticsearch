/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.gen.pipeline;

import org.elasticsearch.xpack.esql.core.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.esql.core.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.HitExtractorProcessor;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.Processor;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class HitExtractorInput extends LeafInput<HitExtractor> {

    public HitExtractorInput(Source source, Expression expression, HitExtractor context) {
        super(source, expression, context);
    }

    @Override
    protected NodeInfo<HitExtractorInput> info() {
        return NodeInfo.create(this, HitExtractorInput::new, expression(), context());
    }

    @Override
    public Processor asProcessor() {
        return new HitExtractorProcessor(context());
    }

    @Override
    public final boolean supportedByAggsOnlyQuery() {
        return true;
    }

    @Override
    public Pipe resolveAttributes(AttributeResolver resolver) {
        return this;
    }

    @Override
    public final void collectFields(QlSourceBuilder sourceBuilder) {
        // No fields to collect
    }
}
