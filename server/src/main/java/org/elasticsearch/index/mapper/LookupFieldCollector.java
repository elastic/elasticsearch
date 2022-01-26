/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsPhase;
import org.elasticsearch.search.fetch.subphase.LookupField;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * A helper class for collecting {@link LookupField} during the {@link FetchFieldsPhase}. Each {@link MappedFieldType}
 * is in charge of defining a lookup field collector through {@link MappedFieldType#lookupFieldCollector(SearchExecutionContext)}.
 *
 * @see LookupRuntimeFieldType#lookupFieldCollector(SearchExecutionContext)
 */
public interface LookupFieldCollector {
    /**
     * Returns the list of {@link LookupField}. These lookup fields will be resolved later.
     *
     * @param inputFieldValues a supplier to provide the input values for fields specified in {@link #inputFields()}.
     * @return the list of lookup fields
     */
    List<LookupField> collect(Function<String, List<Object>> inputFieldValues) throws IOException;

    /**
     * The list of fields whose values fetched by {@link ValueFetcher} can be used by the collector during {@link #collect(Function)}
     */
    List<String> inputFields();
}
