/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.HitExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

public class HitExtractorInput extends LeafInput<HitExtractor> {

    public HitExtractorInput(Expression expression, HitExtractor context) {
        super(expression, context);
    }

    @Override
    public Processor asProcessor() {
        return new HitExtractorProcessor(context());
    }
}
