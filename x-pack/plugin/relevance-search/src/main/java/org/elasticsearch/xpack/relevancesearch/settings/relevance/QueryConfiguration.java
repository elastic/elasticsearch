/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.settings.relevance;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.xpack.relevancesearch.relevance.boosts.AbstractScriptScoreBoost;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryConfiguration {

    private Map<String, Float> fieldsAndBoosts;

    public Map<String, List<AbstractScriptScoreBoost>> scriptScores;

    public Map<String, Float> getFieldsAndBoosts() {
        return fieldsAndBoosts;
    }

    public void setFieldsAndBoosts(Map<String, Float> fieldsAndBoosts) {
        this.fieldsAndBoosts = fieldsAndBoosts;
    }

    public Map<String, List<AbstractScriptScoreBoost>> getScriptScores() {
        return scriptScores;
    }

    public void parseScriptScores(Map<String, List<Map<String, Object>>> scores) {
        if (scores == null) {
            return;
        }

        this.scriptScores = scores.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().map(AbstractScriptScoreBoost::parse).collect(Collectors.toList())
                )
            );
    }

    public void parseFieldsAndBoosts(List<String> inputFields) throws RelevanceSettingsService.InvalidSettingsException {
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
            throw new RelevanceSettingsService.InvalidSettingsException("Invalid boost detected in relevance settings, must be numeric");
        }
    }

    public String getScriptSource() {
        if (this.scriptScores == null || this.scriptScores.isEmpty()) {
            return null;
        }
        String addScores = String.join(" + ", additiveScriptSources());
        String mulScores = String.join(" * ", multiplicativeScriptSources());
        if (addScores.length() > 0 && mulScores.length() > 0) {
            return format("Math.max(_score * ({0}) + ({1}) + _score, _score)", mulScores, addScores);
        } else if (addScores.length() > 0) {
            return format("Math.max({0} + _score, _score)", addScores);
        } else if (mulScores.length() > 0) {
            return format("Math.max(_score * ({0}) + _score, _score)", mulScores);
        }
        return null;
    }

    public List<String> additiveScriptSources() {
        List<String> scriptSources = new ArrayList<>();
        for (String field : this.scriptScores.keySet()) {
            for (AbstractScriptScoreBoost boost : this.scriptScores.get(field)) {
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
            for (AbstractScriptScoreBoost boost : this.scriptScores.get(field)) {
                if (boost.isMultiplicative()) {
                    scriptSources.add(boost.getSource(field));
                }
            }
        }
        return scriptSources;
    }

    private String format(String pattern, Object... arguments) {
        MessageFormat formatter = new MessageFormat(pattern, Locale.ROOT);
        return formatter.format(arguments);
    }
}
