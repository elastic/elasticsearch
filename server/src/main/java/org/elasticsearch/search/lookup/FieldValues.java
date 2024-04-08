/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Represents values for a given document
 */
public interface FieldValues<T> {

    /**
     * Loads the values for the given document and passes them to the consumer
     * @param lookup    a search lookup to access values from
     * @param ctx       the LeafReaderContext containing the document
     * @param doc       the docid
     * @param consumer  called with each document value
     */
    void valuesForDoc(SearchLookup lookup, LeafReaderContext ctx, int doc, Consumer<T> consumer);

    /**
     * Creates a {@link ValueFetcher} that fetches values from a {@link FieldValues} instance
     * @param fieldValues the source of the values
     * @param context the search execution context
     * @return the value fetcher
     */
    static ValueFetcher valueFetcher(FieldValues<?> fieldValues, SearchExecutionContext context) {
        return valueFetcher(fieldValues, v -> v, context);
    }

    /**
     * Creates a {@link ValueFetcher} that fetches values from a {@link FieldValues} instance
     * @param fieldValues the source of the values
     * @param formatter   a function to format the values
     * @param context the search execution context
     * @return the value fetcher
     */
    static ValueFetcher valueFetcher(FieldValues<?> fieldValues, Function<Object, Object> formatter, SearchExecutionContext context) {
        return new ValueFetcher() {
            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext context) {
                this.ctx = context;
            }

            @Override
            public List<Object> fetchValues(Source lookup, int doc, List<Object> ignoredValues) {
                List<Object> values = new ArrayList<>();
                try {
                    fieldValues.valuesForDoc(context.lookup(), ctx, doc, v -> values.add(formatter.apply(v)));
                } catch (Exception e) {
                    ignoredValues.addAll(values);
                }
                return values;
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;       // TODO can we get more information from the script
            }
        };
    }

    /**
     * Creates a {@link ValueFetcher} that fetches values from a {@link FieldValues} instance
     * @param fieldValues the source of the values
     * @param formatter   a function to format the list values
     * @param context the search execution context
     * @return the value fetcher
     */
    static <T> ValueFetcher valueListFetcher(
        FieldValues<T> fieldValues,
        Function<List<T>, List<Object>> formatter,
        SearchExecutionContext context
    ) {
        return new ValueFetcher() {
            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext context) {
                this.ctx = context;
            }

            @Override
            public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) {
                List<T> values = new ArrayList<>();
                try {
                    fieldValues.valuesForDoc(context.lookup(), ctx, doc, values::add);
                } catch (Exception e) {
                    ignoredValues.addAll(values);
                }
                return formatter.apply(values);
            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return StoredFieldsSpec.NEEDS_SOURCE;   // TODO can we get more info from the script?
            }
        };
    }
}
