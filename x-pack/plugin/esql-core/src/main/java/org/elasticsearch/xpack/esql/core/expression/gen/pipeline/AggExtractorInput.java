/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.gen.pipeline;

import org.elasticsearch.xpack.esql.core.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.esql.core.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.BucketExtractorProcessor;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.esql.core.expression.gen.processor.Processor;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

public class AggExtractorInput extends LeafInput<BucketExtractor> {

    private final Processor chained;

    public AggExtractorInput(Source source, Expression expression, Processor processor, BucketExtractor context) {
        super(source, expression, context);
        this.chained = processor;
    }

    @Override
    protected NodeInfo<AggExtractorInput> info() {
        return NodeInfo.create(this, AggExtractorInput::new, expression(), chained, context());
    }

    @Override
    public Processor asProcessor() {
        Processor proc = new BucketExtractorProcessor(context());
        return chained != null ? new ChainingProcessor(proc, chained) : proc;
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
        // Nothing to collect
    }
}
