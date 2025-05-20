/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.simplified;

import org.elasticsearch.index.search.QueryParserHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimplifiedInnerRetrieverParser {
    private SimplifiedInnerRetrieverParser() {}

    public static List<SimplifiedInnerRetrieverInfo> parse(List<String> fieldsAndWeights, String query) {
        Map<String, Float> parsedFieldsAndWeights = QueryParserHelper.parseFieldsAndWeights(fieldsAndWeights);
        List<SimplifiedInnerRetrieverInfo> simplifiedInnerRetrieverInfo = new ArrayList<>(parsedFieldsAndWeights.size());
        for (Map.Entry<String, Float> entry : parsedFieldsAndWeights.entrySet()) {
            simplifiedInnerRetrieverInfo.add(new SimplifiedInnerRetrieverInfo(entry.getKey(), entry.getValue(), query));
        }
        return simplifiedInnerRetrieverInfo;
    }
}
