/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.simplified;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SimplifiedInnerRetrieverUtils {
    private SimplifiedInnerRetrieverUtils() {}

    public static List<CompoundRetrieverBuilder.RetrieverSource> convertToRetrievers(Collection<String> fields, String query) {
        List<CompoundRetrieverBuilder.RetrieverSource> retrievers = new ArrayList<>(fields.size());
        for (String field : fields) {
            RetrieverBuilder retrieverBuilder = new StandardRetrieverBuilder(new MatchQueryBuilder(field, query));
            retrievers.add(new CompoundRetrieverBuilder.RetrieverSource(retrieverBuilder, null));
        }
        return retrievers;
    }
}
