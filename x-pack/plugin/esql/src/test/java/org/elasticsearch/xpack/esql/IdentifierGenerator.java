/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThan;

public class IdentifierGenerator {

    /**
     * Generate random identifier that could be used as a column name
     */
    public static String randomIdentifier() {
        return ESTestCase.randomIdentifier();
    }

    /**
     * Generates one or several coma separated index patterns
     */
    public static String randomIndexPatterns(Feature... features) {
        return maybeQuote(String.join(",", randomList(1, 5, () -> randomIndexPattern(features))));
    }

    /**
     * Generates a random valid index pattern.
     * You may force list of features to be included or excluded using the arguments, eg {@code randomIndexPattern(PATTERN, not(HIDDEN))}.
     * Identifier could be an index or alias. It might be hidden or remote or use a pattern.
     * See @link <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params">valid index patterns</a>
     */
    public static String randomIndexPattern(Feature... features) {
        var validFirstCharacters = "abcdefghijklmnopqrstuvwxyz0123456789!'$^&";
        var validCharacters = validFirstCharacters + "+-_.";

        var index = new StringBuilder();
        if (canAdd(Features.HIDDEN_INDEX, features)) {
            index.append('.');
        }
        index.append(randomCharacterFrom(validFirstCharacters));
        for (int i = 0; i < randomIntBetween(1, 100); i++) {
            index.append(randomCharacterFrom(validCharacters));
        }
        if (canAdd(Features.WILDCARD_PATTERN, features)) {
            if (randomBoolean()) {
                index.append('*');
            } else {
                for (int i = 0; i < randomIntBetween(1, 3); i++) {
                    index.insert(randomIntBetween(0, index.length()), '*');
                }
            }
        }
        if (canAdd(Features.DATE_MATH, features)) {
            // https://www.elastic.co/guide/en/elasticsearch/reference/8.17/api-conventions.html#api-date-math-index-names
            index.insert(0, "<");
            index.append("-{now/");
            index.append(randomFrom("d", "M", "M-1M"));
            if (randomBoolean()) {
                index.append("{").append(switch (randomIntBetween(0, 2)) {
                    case 0 -> "yyyy.MM";
                    case 1 -> "yyyy.MM.dd";
                    default -> "yyyy.MM.dd|" + Strings.format("%+03d", randomValueOtherThan(0, () -> randomIntBetween(-18, 18))) + ":00";
                }).append("}");
            }
            index.append("}>");
        }
        if (canAdd(Features.EXCLUDE_PATTERN, features)) {
            index.insert(0, "-");
        }

        boolean requiresQuote = false;
        var pattern = index.toString();
        if (pattern.contains("|")) {
            requiresQuote = true;
        }

        if (canAdd(Features.CROSS_CLUSTER, features)) {
            var cluster = randomIdentifier();
            pattern = cluster + ":" + pattern;
        } else if (EsqlCapabilities.Cap.INDEX_COMPONENT_SELECTORS.isEnabled() && canAdd(Features.INDEX_SELECTOR, features)) {
            pattern = pattern + "::" + randomFrom(IndexComponentSelector.values()).getKey();
        }

        if (requiresQuote) {
            pattern = quote(pattern);
        }

        return pattern;
    }

    private static char randomCharacterFrom(String str) {
        return str.charAt(randomInt(str.length() - 1));
    }

    public interface Feature {}

    public enum Features implements Feature {
        CROSS_CLUSTER,
        HIDDEN_INDEX,
        WILDCARD_PATTERN,
        EXCLUDE_PATTERN,
        DATE_MATH,
        INDEX_SELECTOR
    }

    private record ExcludedFeature(Feature feature) implements Feature {}

    public static Feature without(Feature feature) {
        return new ExcludedFeature(feature);
    }

    private static boolean canAdd(Feature feature, Feature... features) {
        for (var f : features) {
            if (f.equals(feature)) {
                return true;
            }
            if (f.equals(without(feature))) {
                return false;
            }
        }
        return randomBoolean();
    }

    public static String maybeQuote(String term) {
        return randomBoolean() && term.contains("\"") == false ? quote(term) : term;
    }

    public static String quote(String term) {
        var quote = randomFrom("\"", "\"\"\"");
        return quote + term + quote;
    }

    public static String unquoteIndexPattern(String term) {
        return term.replace("\"\"\"", "").replace("\"", "");
    }
}
