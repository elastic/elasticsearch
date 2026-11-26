/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Sink operator that calls a given listener for each page received. The listener receives both the page as well as schema information,
 * i.e. the names of the rows that are outputted.
 */
public class OutputOperator extends SinkOperator {

    /**
     * Interface for DriverContext implementations that provide a collectedPages list.
     * This allows OutputOperatorFactory to retrieve collectedPages without creating a circular dependency.
     */
    public interface CollectedPagesProvider {
        /**
         * Returns the collectedPages list that should be used by OutputOperator.
         * @return the collectedPages list
         */
        List<Page> collectedPages();
    }

    private final List<String> columns;
    private final PageConsumer pageConsumer;
    private final Function<Page, Page> mapper;

    public record OutputOperatorFactory(List<String> columns, Function<Page, Page> mapper, PageConsumer.Factory pageConsumerFactory)
        implements
            SinkOperatorFactory {

        public OutputOperatorFactory(List<String> columns, Function<Page, Page> mapper, Consumer<Page> pageConsumer) {
            this(columns, mapper, PageConsumer.Factory.from(pageConsumer));
        }

        @Override
        public SinkOperator get(DriverContext driverContext) {
            // If this DriverContext provides collectedPages, use them
            if (driverContext instanceof CollectedPagesProvider collectedPagesProvider) {
                List<Page> collectedPages = collectedPagesProvider.collectedPages();
                return new OutputOperator(columns, mapper, collectedPages::add);
            } else {
                // Normal execution path: use the provided pageConsumer
                return new OutputOperator(columns, mapper, pageConsumerFactory.create(driverContext));
            }
        }

        @Override
        public String describe() {
            return OutputOperator.describe(columns);
        }
    }

    public OutputOperator(List<String> columns, Function<Page, Page> mapper, Consumer<Page> pageConsumer) {
        this(columns, mapper, PageConsumer.from(pageConsumer));
    }

    public OutputOperator(List<String> columns, Function<Page, Page> mapper, PageConsumer pageConsumer) {
        this.columns = columns;
        this.mapper = mapper;
        this.pageConsumer = pageConsumer;
    }

    public List<String> getColumns() {
        return columns;
    }

    boolean finished = false;

    @Override
    public boolean isFinished() {
        return finished && pageConsumer.isFinished();
    }

    @Override
    public void finish() {
        finished = true;
        pageConsumer.finish();
    }

    @Override
    public boolean needsInput() {
        return finished == false && isBlocked().listener().isDone();
    }

    @Override
    public IsBlockedResult isBlocked() {
        return pageConsumer.isBlocked();
    }

    @Override
    protected void doAddInput(Page page) {
        pageConsumer.consume(mapper.apply(page));
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return describe(columns);
    }

    private static String describe(List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("OutputOperator").append("[");
        sb.append("columns = ");
        if (columns.size() <= 10) {
            sb.append(columns);
        } else {
            sb.append('[').append(columns.size()).append(" columns").append(']');
        }
        sb.append("]");
        return sb.toString();
    }
}
