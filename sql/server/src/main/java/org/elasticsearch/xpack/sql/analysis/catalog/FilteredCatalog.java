/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

/**
 * {@link Catalog} implementation that filters the results.
 */
public class FilteredCatalog implements Catalog {
    public interface Filter {
        /**
         * Filter an index. Returning {@code null} will act as though
         * the index wasn't found. Will never be called with a {@code null}
         * parameter.
         */
        EsIndex filterIndex(EsIndex index);
    }

    private Catalog delegate;
    private Filter filter;

    public FilteredCatalog(Catalog delegate, Filter filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public EsIndex getIndex(String index) {
        EsIndex result = delegate.getIndex(index);
        if (result == null) {
            return null;
        }
        return filter.filterIndex(result);
    }
}
