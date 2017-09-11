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
         * Filter an index. Will only be called with valid,
         * found indices but gets the entire {@link GetIndexResult}
         * from the delegate catalog in case it wants to return
         * it unchanged.
         */
        GetIndexResult filterIndex(GetIndexResult delegateResult);
    }

    private Catalog delegate;
    private Filter filter;

    public FilteredCatalog(Catalog delegate, Filter filter) {
        this.delegate = delegate;
        this.filter = filter;
    }

    @Override
    public GetIndexResult getIndex(String index) {
        GetIndexResult result = delegate.getIndex(index);
        if (false == result.isValid() || result.get() == null) {
            return result;
        }
        return filter.filterIndex(result);
    }
}
