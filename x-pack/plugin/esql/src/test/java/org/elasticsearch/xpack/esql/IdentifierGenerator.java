/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

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
        return maybeQuote(String.join(",", ESTestCase.randomList(1, 5, () -> randomIndexPattern(features))));
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
        for (int i = 0; i < ESTestCase.randomIntBetween(1, 100); i++) {
            index.append(randomCharacterFrom(validCharacters));
        }
        if (canAdd(Features.WILDCARD_PATTERN, features)) {
            if (ESTestCase.randomBoolean()) {
                index.append('*');
            } else {
                index.insert(ESTestCase.randomIntBetween(0, index.length() - 1), '*');
            }
        } else if (canAdd(Features.DATE_MATH, features)) {
            // https://www.elastic.co/guide/en/elasticsearch/reference/8.17/api-conventions.html#api-date-math-index-names
            index.insert(0, "<");
            index.append("-{now/");
            index.append(ESTestCase.randomFrom("d", "M", "M-1M"));
            if (ESTestCase.randomBoolean()) {
                index.append("{").append(ESTestCase.randomFrom("yyyy.MM", "yyyy.MM.dd")).append("}");
            }
            index.append("}>");
        }

        var pattern = maybeQuote(index.toString());
        if (canAdd(Features.CROSS_CLUSTER, features)) {
            var cluster = randomIdentifier();
            pattern = maybeQuote(cluster + ":" + pattern);
        }
        return pattern;
    }

    private static char randomCharacterFrom(String str) {
        return str.charAt(ESTestCase.randomInt(str.length() - 1));
    }

    public interface Feature {}

    public enum Features implements Feature {
        CROSS_CLUSTER,
        WILDCARD_PATTERN,
        DATE_MATH,
        HIDDEN_INDEX
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
        return ESTestCase.randomBoolean();
    }

    public static String maybeQuote(String term) {
        if (term.contains("\"")) {
            return term;
        }
        return switch (ESTestCase.randomIntBetween(0, 5)) {
            case 0 -> "\"" + term + "\"";
            case 1 -> "\"\"\"" + term + "\"\"\"";
            default -> term;// no quotes are more likely
        };
    }

    public static String unquoteIndexPattern(String term) {
        return term.replace("\"\"\"", "").replace("\"", "");
    }
}
