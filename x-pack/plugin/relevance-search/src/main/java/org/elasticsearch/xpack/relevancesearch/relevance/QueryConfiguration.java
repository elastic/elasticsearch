/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.ScriptScoreBoost;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryConfiguration {

    private Map<String, Float> fieldsAndBoosts;

    public Map<String, List<ScriptScoreBoost>> scriptScores;

    public Map<String, Float> getFieldsAndBoosts() {
        return fieldsAndBoosts;
    }

    public void setFieldsAndBoosts(Map<String, Float> fieldsAndBoosts) {
        this.fieldsAndBoosts = fieldsAndBoosts;
    }

    public Map<String, List<ScriptScoreBoost>> getScriptScores() {
        return scriptScores;
    }

    public void parseScriptScores(Map<String, List<Map<String, Object>>> scores) {
        if (scores == null) {
            return;
        }
        Map<String, List<ScriptScoreBoost>> result = new HashMap<>();
        for (String field : scores.keySet()) {
            List<ScriptScoreBoost> fieldScores = new ArrayList<>();
            for (Map<String, Object> boostProps : scores.get(field)) {
                fieldScores.add(ScriptScoreBoost.parse(boostProps));
            }
            result.put(field, fieldScores);
        }
        this.scriptScores = result;
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

    public String getScriptSource() {
        if (this.scriptScores == null || this.scriptScores.isEmpty()) {
            return null;
        }
        String addScores = String.join(" + ", additiveScriptSources());
        String mulScores = String.join(" * ", multiplicativeScriptSources());
        if (addScores.length() > 0 && mulScores.length() > 0) {
            return MessageFormat.format("Math.max(_score * ({0}) + ({1}) + _score, _score)", mulScores, addScores);
        } else if (addScores.length() > 0) {
            return MessageFormat.format("Math.max({0} + _score, _score)", addScores);
        } else if (mulScores.length() > 0) {
            return MessageFormat.format("Math.max(_score * ({0}) + _score, _score)", mulScores);
        }
        return null;
    }

    public List<String> additiveScriptSources() {
        List<String> scriptSources = new ArrayList<>();
        for (String field : this.scriptScores.keySet()) {
            for (ScriptScoreBoost boost : this.scriptScores.get(field)) {
                if (boost.isAdditive()) {
                    scriptSources.add(boost.getSource(field));
                }
            }
        }
        return scriptSources;
    }

    public List<String> multiplicativeScriptSources() {
        List<String> scriptSources = new ArrayList<>();
        for (String field : this.scriptScores.keySet()) {
            for (ScriptScoreBoost boost : this.scriptScores.get(field)) {
                if (boost.isMultiplicative()) {
                    scriptSources.add(boost.getSource(field));
                }
            }
        }
        return scriptSources;
    }
}
