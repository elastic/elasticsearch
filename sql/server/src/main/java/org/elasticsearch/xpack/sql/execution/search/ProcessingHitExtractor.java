/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;

class ProcessingHitExtractor implements HitExtractor {

    final HitExtractor delegate;
    private final ColumnProcessor processor;

    ProcessingHitExtractor(HitExtractor delegate, ColumnProcessor processor) {
        this.delegate = delegate;
        this.processor = processor;
    }

    @Override
    public Object get(SearchHit hit) {
        return processor.apply(delegate.get(hit));
    }
}
