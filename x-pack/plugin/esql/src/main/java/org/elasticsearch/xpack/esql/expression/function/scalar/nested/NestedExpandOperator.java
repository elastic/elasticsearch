/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Emits one row per object of a mapped nested field. Parent blocks are repeated once per object; one block per child
 * field is appended. A parent without objects keeps one row with null child values. An input page is given back as pages
 * of at most {@code pageSize} rows, so a parent with many objects does not need all of them in one page.
 */
public final class NestedExpandOperator extends AbstractPageMappingToIteratorOperator {
    public record Factory(int docChannel, NestedChildren.Factory children, String path, int pageSize) implements Operator.OperatorFactory {
        public Factory {
            if (pageSize < 1) {
                throw new IllegalArgumentException("pageSize must be positive but was [" + pageSize + "]");
            }
        }

        @Override
        public Operator get(DriverContext context) {
            return new NestedExpandOperator(context, this);
        }

        @Override
        public String describe() {
            return "NestedExpandOperator[path=" + path + "]";
        }
    }

    private final Factory factory;
    private final NestedChildren children;

    private NestedExpandOperator(DriverContext context, Factory factory) {
        this.factory = factory;
        this.children = factory.children().get(context);
    }

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        DocVector docs = ((DocBlock) page.getBlock(factory.docChannel())).asVector();
        return new Expanded(page, children.expand(docs, factory.pageSize()));
    }

    /** Pages of the rows of {@code page} with its objects. Owns {@code page} until the last rows are given or it is closed. */
    private static final class Expanded implements ReleasableIterator<Page> {
        private final Page page;
        private final NestedChildren.Expansion expansion;
        private boolean released;

        private Expanded(Page page, NestedChildren.Expansion expansion) {
            this.page = page;
            this.expansion = expansion;
        }

        @Override
        public boolean hasNext() {
            return released == false && expansion.hasNext();
        }

        @Override
        public Page next() {
            NestedChildren.Rows rows = expansion.next();
            Page parents = null;
            Page result = null;
            try {
                parents = page.filter(true, rows.positions(), rows.offset(), rows.count());
                result = parents.appendBlocks(rows.fields());
                parents = null;
            } finally {
                if (result == null) {
                    Releasables.closeExpectNoException(rows.fields());
                    if (parents != null) {
                        parents.releaseBlocks();
                    }
                }
            }
            if (expansion.hasNext() == false) {
                close();
            }
            return result;
        }

        @Override
        public void close() {
            if (released == false) {
                released = true;
                Releasables.closeExpectNoException(page::releaseBlocks, expansion);
            }
        }
    }

    @Override
    public String toString() {
        return factory.describe();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(children, super::close);
    }
}
