/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.planner.FetchOperatorProvider;

import java.util.List;

/**
 * The coordinator's operator for a {@link FetchExec}. It runs plans whose cut keeps no rows, like the plans
 * {@code EXPLAIN} runs over empty sources, and fails on the first row because it can't load documents.
 */
public final class FetchOperator extends AbstractPageMappingOperator {
    /**
     * Plans each {@link FetchExec} into this operator.
     */
    public static final FetchOperatorProvider PROVIDER = (exec, docRefChannel, fetchedTypes) -> new Factory(docRefChannel, fetchedTypes);

    /**
     * @param docRefChannel the input channel that holds the document references
     * @param fetchedTypes  the element type of each fetched column
     */
    public record Factory(int docRefChannel, List<ElementType> fetchedTypes) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new FetchOperator(docRefChannel, fetchedTypes);
        }

        @Override
        public String describe() {
            return "FetchOperator[docRefChannel=" + docRefChannel + ", fetchedTypes=" + fetchedTypes + "]";
        }
    }

    private final int docRefChannel;
    private final List<ElementType> fetchedTypes;

    FetchOperator(int docRefChannel, List<ElementType> fetchedTypes) {
        this.docRefChannel = docRefChannel;
        this.fetchedTypes = fetchedTypes;
    }

    @Override
    protected Page process(Page page) {
        // the page stays with the operator, which releases it on close
        throw new IllegalStateException("the fetch phase can't load documents yet");
    }

    @Override
    public String toString() {
        return "FetchOperator[docRefChannel=" + docRefChannel + ", fetchedTypes=" + fetchedTypes + "]";
    }
}
