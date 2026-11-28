/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Runs a source residual on each page a parquet reader emits. The reader has already applied its native filter.
 * The residual uses the expression evaluator {@code FilterExec} uses, so a null comparison drops the row.
 */
final class SourceResidualPages implements CloseableIterator<Page> {
    static CloseableIterator<Page> wrap(
        CloseableIterator<Page> pages,
        ParquetPushedExpressions pushed,
        List<Attribute> attributes,
        BlockFactory blockFactory
    ) {
        if (pushed == null || pushed.sourceResidual().isEmpty()) {
            return pages;
        }
        return new SourceResidualPages(pages, pushed.sourceResidual(), attributes, blockFactory);
    }

    private final CloseableIterator<Page> pages;
    private final FilterOperator filter;
    private Page pending;
    private boolean finished;

    private SourceResidualPages(
        CloseableIterator<Page> pages,
        List<Expression> residual,
        List<Attribute> attributes,
        BlockFactory blockFactory
    ) {
        this.pages = pages;
        Map<String, Attribute> byName = new HashMap<>();
        for (Attribute attribute : attributes) {
            byName.put(attribute.name(), attribute);
        }
        Expression combined = Predicates.combineAnd(residual).transformUp(Attribute.class, attribute -> {
            Attribute onPage = byName.get(attribute.name());
            if (onPage == null) {
                throw new IllegalStateException("parquet residual reads [" + attribute.name() + "] and that column is not on the page");
            }
            return onPage;
        });
        var evaluator = EvalMapper.toEvaluator(FoldContext.small(), combined, new Layout.Builder().append(attributes).build());
        this.filter = new FilterOperator(evaluator.get(new DriverContext(blockFactory.bigArrays(), blockFactory, null)));
    }

    @Override
    public boolean hasNext() {
        if (pending != null) {
            return true;
        }
        if (finished) {
            return false;
        }
        pending = pull();
        finished = pending == null;
        return pending != null;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page page = pending;
        pending = null;
        return page;
    }

    private Page pull() {
        while (pages.hasNext()) {
            Page page = pages.next();
            if (page.getPositionCount() == 0) {
                return page;
            }
            filter.addInput(page);
            Page kept = filter.getOutput();
            if (kept != null) {
                return kept;
            }
        }
        return null;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(filter);
        try {
            IOUtils.close(pages);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
