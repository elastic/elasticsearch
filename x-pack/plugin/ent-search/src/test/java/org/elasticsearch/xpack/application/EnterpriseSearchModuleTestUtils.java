/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.rules.QueryRule;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteria;
import org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationTemplate;
import org.elasticsearch.xpack.application.search.TemplateParamValidator;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomMap;
import static org.elasticsearch.xpack.application.rules.QueryRule.MAX_PRIORITY;
import static org.elasticsearch.xpack.application.rules.QueryRule.MIN_PRIORITY;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.ALWAYS;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;

public final class EnterpriseSearchModuleTestUtils {

    private EnterpriseSearchModuleTestUtils() {
        throw new UnsupportedOperationException("Don't instantiate this class!");
    }

    public static PageParams randomPageParams() {
        int from = randomIntBetween(0, 10000);
        int size = randomIntBetween(0, 10000);
        return new PageParams(from, size);
    }

    public static SearchApplication randomSearchApplication() {
        return new SearchApplication(
            ESTestCase.randomAlphaOfLengthBetween(1, 10),
            generateRandomStringArray(10, 10, false, false),
            randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) }),
            randomLongBetween(0, Long.MAX_VALUE),
            randomBoolean() ? getRandomSearchApplicationTemplate() : null
        );
    }

    public static SearchApplicationTemplate getRandomSearchApplicationTemplate() {
        String paramName = randomAlphaOfLengthBetween(8, 10);
        String paramValue = randomAlphaOfLengthBetween(8, 10);
        String query = String.format(Locale.ROOT, """
            "query_string": {
                "query": "{{%s}}"
            }
            """, paramName);
        final Script script = new Script(ScriptType.INLINE, "mustache", query, Collections.singletonMap(paramName, paramValue));
        String paramValidationSource = String.format(Locale.ROOT, """
            {
                "%s": {
                    "type": "string"
                }
            }
            """, paramName);
        final TemplateParamValidator templateParamValidator = new TemplateParamValidator(paramValidationSource);
        return new SearchApplicationTemplate(script, templateParamValidator);
    }

    public static Map<String, Object> randomSearchApplicationQueryParams() {
        return randomMap(0, 10, () -> Tuple.tuple(randomIdentifier(), randomAlphaOfLengthBetween(0, 10)));
    }

    public static QueryRuleCriteria randomQueryRuleCriteria() {
        QueryRuleCriteriaType criteriaType = randomFrom(QueryRuleCriteriaType.values());
        String metadata = randomAlphaOfLength(10);
        List<Object> values = new ArrayList<>();
        int numValues = randomIntBetween(1, 3);

        if (criteriaType.isNumericComparison()) {
            for (int i = 0; i < numValues; i++) {
                double value = Math.round(randomDoubleBetween(0, 1000, true) * 100.0) / 100.0;
                values.add(String.valueOf(value));
            }
        } else if (criteriaType == QueryRuleCriteriaType.ALWAYS) {
            metadata = null;
            values = null;
        } else {
            for (int i = 0; i < numValues; i++) {
                values.add(randomAlphaOfLength(5));
            }
        }

        return new QueryRuleCriteria(criteriaType, metadata, values);
    }

    public static QueryRule randomQueryRule() {
        if (randomBoolean()) {
            return createNumericQueryRule();
        } else {
            return createNonNumericQueryRule();
        }
    }

    private static QueryRule createNumericQueryRule() {
        String ruleId = randomAlphaOfLength(10);
        QueryRule.QueryRuleType type = randomFrom(QueryRule.QueryRuleType.values());
        List<QueryRuleCriteria> criteria = new ArrayList<>();
        int numCriteria = randomIntBetween(1, 3);

        for (int i = 0; i < numCriteria; i++) {
            QueryRuleCriteriaType criteriaType = randomFrom(
                QueryRuleCriteriaType.GT,
                QueryRuleCriteriaType.GTE,
                QueryRuleCriteriaType.LT,
                QueryRuleCriteriaType.LTE
            );
            double value = Math.round(randomDoubleBetween(0, 1000, true) * 100.0) / 100.0;
            criteria.add(new QueryRuleCriteria(criteriaType, randomAlphaOfLength(10), List.of(String.valueOf(value))));
        }

        return new QueryRule(ruleId, type, criteria, randomQueryRuleActions(), randomQueryRulePriority());
    }

    private static QueryRule createNonNumericQueryRule() {
        String ruleId = randomAlphaOfLength(10);
        QueryRule.QueryRuleType type = randomFrom(QueryRule.QueryRuleType.values());
        List<QueryRuleCriteria> criteria = new ArrayList<>();
        int numCriteria = randomIntBetween(1, 3);

        for (int i = 0; i < numCriteria; i++) {
            QueryRuleCriteriaType criteriaType = randomFrom(
                QueryRuleCriteriaType.EXACT,
                QueryRuleCriteriaType.PREFIX,
                QueryRuleCriteriaType.CONTAINS,
                QueryRuleCriteriaType.ALWAYS
            );

            String metadata = criteriaType == QueryRuleCriteriaType.ALWAYS ? null : randomAlphaOfLength(10);
            List<Object> values = criteriaType == QueryRuleCriteriaType.ALWAYS ?
                null :
                List.of(randomAlphaOfLength(5));

            criteria.add(new QueryRuleCriteria(criteriaType, metadata, values));
        }

        return new QueryRule(ruleId, type, criteria, randomQueryRuleActions(), randomQueryRulePriority());
    }

    public static Integer randomQueryRulePriority() {
        return randomBoolean() ? randomIntBetween(MIN_PRIORITY, MAX_PRIORITY) : null;
    }

    public static QueryRuleset randomQueryRuleset() {
        String id = randomAlphaOfLengthBetween(1, 10);
        int numRules = randomIntBetween(1, 10);
        List<QueryRule> rules = new ArrayList<>(numRules);
        for (int i = 0; i < numRules; i++) {
            rules.add(randomQueryRule());
        }
        return new QueryRuleset(id, rules);
    }

    public static Map<String, Object> randomMatchCriteria() {
        return randomMap(1, 3, () -> Tuple.tuple(randomIdentifier(), randomAlphaOfLengthBetween(0, 10)));
    }

    public static Map<String, Object> randomQueryRuleActions() {
        QueryRule.QueryRuleType type = randomFrom(QueryRule.QueryRuleType.values());
        if (type == QueryRule.QueryRuleType.PINNED) {
            // For PINNED type, we need either 'ids' or 'docs', but not both
            if (randomBoolean()) {
                return Map.of("ids", List.of(randomAlphaOfLength(5)));
            } else {
                return Map.of("docs", List.of(Map.of("_index", randomAlphaOfLength(5), "_id", randomAlphaOfLength(5))));
            }
        } else if (type == QueryRule.QueryRuleType.EXCLUDE) {
            // For EXCLUDE type, similar to PINNED
            if (randomBoolean()) {
                return Map.of("ids", List.of(randomAlphaOfLength(5)));
            } else {
                return Map.of("docs", List.of(Map.of("_index", randomAlphaOfLength(5), "_id", randomAlphaOfLength(5))));
            }
        } else {
            // For other types (like BOOST), use a boost value
            return Map.of("boost", randomDoubleBetween(0.0, 5.0, true));
        }
    }

}
