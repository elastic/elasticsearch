/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.ValuesLookup;

import java.io.IOException;
import java.util.List;

/**
 * Value fetcher that loads from doc values.
 */
public final class DocValueFetcher implements ValueFetcher {
    private final DocValueFormat format;
    private final String field;

    public DocValueFetcher(DocValueFormat format, String field) {
        this.format = format;
        this.field = field;
    }

    @Override
    public List<Object> fetchValues(ValuesLookup lookup) throws IOException {
        return lookup.docValues(field, format);
    }

}
