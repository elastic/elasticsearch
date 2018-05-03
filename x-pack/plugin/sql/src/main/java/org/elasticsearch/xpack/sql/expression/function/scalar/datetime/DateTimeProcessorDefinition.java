/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.VarArgsProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;

public class DateTimeProcessorDefinition extends VarArgsProcessorDefinition {

    private DateTimeProcessor.DateTimeExtractor extractor;
    private FunctionContext context;

    DateTimeProcessorDefinition(Location location,
                                Expression expression,
                                List<ProcessorDefinition> definitions,
                                DateTimeProcessor.DateTimeExtractor extractor,
                                FunctionContext context ) {
        super(location, expression, definitions);
        extractor.checkArgumentCount(definitions);
        this.extractor = extractor;
        this.context = context;
    }

    @Override
    protected Processor asProcessor(final List<Processor> processors) {
        return new DateTimeProcessor(processors, extractor, context);
    }

    @Override
    public ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        return new DateTimeProcessorDefinition(this.location(),
            this.expression(),
            newChildren,
            this.extractor,
            this.context);
    }

    @Override
    protected NodeInfo<? extends ProcessorDefinition> info() {
        return NodeInfo.create(this,
            DateTimeProcessorDefinition::new,
            expression(),
            this.children(),
            this.extractor,
            this.context);
    }
}
