/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.relevance.settings.RelevanceSettingsService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryConfiguration {

    private Map<String, Float> fieldsAndBoosts;

    public Map<String, Float> getFieldsAndBoosts() {
        return fieldsAndBoosts;
    }

    public void setFieldsAndBoosts(Map<String, Float> fieldsAndBoosts) {
        this.fieldsAndBoosts = fieldsAndBoosts;
    }

    public void parseFieldsAndBoosts(List<String> inputFields) throws RelevanceSettingsService.RelevanceSettingsInvalidException {
        try {
            this.fieldsAndBoosts = inputFields.stream()
                .map(s -> s.split("\\^"))
                .collect(
                    Collectors.toMap(
                        field -> field[0],
                        boost -> boost.length > 1 ? Float.parseFloat(boost[1]) : AbstractQueryBuilder.DEFAULT_BOOST
                    )
                );
        } catch (NumberFormatException e) {
            throw new RelevanceSettingsService.RelevanceSettingsInvalidException(
                "Invalid boost detected in relevance settings, must be numeric"
            );
        }
    }
}
