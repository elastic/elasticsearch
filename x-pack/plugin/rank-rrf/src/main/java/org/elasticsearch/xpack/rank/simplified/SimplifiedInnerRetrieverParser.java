/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.simplified;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SimplifiedInnerRetrieverParser {
    private SimplifiedInnerRetrieverParser() {}

    public static List<SimplifiedInnerRetrieverInfo> parse(List<String> fieldsAndWeights, String query) {
        return parse(fieldsAndWeights, query, null);
    }

    public static List<SimplifiedInnerRetrieverInfo> parse(List<String> fieldsAndWeights, String query, Consumer<Float> weightValidator) {
        Map<String, Float> parsedFieldsAndWeights = QueryParserHelper.parseFieldsAndWeights(fieldsAndWeights);
        List<SimplifiedInnerRetrieverInfo> simplifiedInnerRetrieverInfo = new ArrayList<>(parsedFieldsAndWeights.size());
        for (Map.Entry<String, Float> entry : parsedFieldsAndWeights.entrySet()) {
            String field = entry.getKey();
            Float weight = entry.getValue();

            if (weightValidator != null) {
                weightValidator.accept(weight);
            }
            simplifiedInnerRetrieverInfo.add(new SimplifiedInnerRetrieverInfo(field, weight, query));
        }
        return simplifiedInnerRetrieverInfo;
    }

    public static List<CompoundRetrieverBuilder.RetrieverSource> convertToRetrievers(
        List<SimplifiedInnerRetrieverInfo> simplifiedInnerRetrieverInfo
    ) {
        List<CompoundRetrieverBuilder.RetrieverSource> retrievers = new ArrayList<>(simplifiedInnerRetrieverInfo.size());
        for (SimplifiedInnerRetrieverInfo info : simplifiedInnerRetrieverInfo) {
            RetrieverBuilder retrieverBuilder = new StandardRetrieverBuilder(new MatchQueryBuilder(info.field(), info.query()));
            retrievers.add(new CompoundRetrieverBuilder.RetrieverSource(retrieverBuilder, null));
        }
        return retrievers;
    }
}
