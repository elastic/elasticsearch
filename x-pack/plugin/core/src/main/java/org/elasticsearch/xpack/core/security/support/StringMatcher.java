/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class acts as a facade / encapsulation around the expression and testing of string-based patterns within Elasticsearch security.
 * Security supports "wildcards" in a number of places (e.g. index names within roles). These cases also support
 * {@link org.apache.lucene.util.automaton.RegExp Lucene-syntax regular expressions} and are implemented via Lucene
 * {@link org.apache.lucene.util.automaton.Automaton} objects.
 * However, it can be more efficient to have special handling and avoid {@code Automata} for particular cases such as exact string matches.
 * This class handles that logic, an provides a clean interface for
 * <em>test whether a provided string matches one of an existing set of patterns</em> that hides the possible implementation options.
 */
public class StringMatcher implements Predicate<String> {

    private static final StringMatcher MATCH_NOTHING = new StringMatcher("(empty)", s -> false);

    protected static final Predicate<String> ALWAYS_TRUE_PREDICATE = s -> true;

    private final String description;
    private final Predicate<String> predicate;
    private static final Logger LOGGER = LogManager.getLogger(StringMatcher.class);

    private StringMatcher(String description, Predicate<String> predicate) {
        this.description = description;
        this.predicate = predicate;
    }

    public static StringMatcher of(Iterable<String> patterns) {
        return StringMatcher.builder().includeAll(patterns).build();
    }

    public static StringMatcher of(String... patterns) {
        return StringMatcher.builder().includeAll(patterns).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public boolean test(String s) {
        return predicate.test(s);
    }

    public boolean isTotal() {
        return predicate == ALWAYS_TRUE_PREDICATE;
    }

    // For testing
    Predicate<String> getPredicate() {
        return predicate;
    }

    @Override
    public StringMatcher or(Predicate<? super String> other) {
        Objects.requireNonNull(other);
        return new StringMatcher(description + "|" + other, this.predicate.or(other));
    }

    @Override
    public StringMatcher and(Predicate<? super String> other) {
        return this.and(String.valueOf(other), other);
    }

    public StringMatcher and(String otherDescription, Predicate<? super String> otherPredicate) {
        Objects.requireNonNull(otherPredicate);
        return new StringMatcher(this.description + "&" + otherDescription, this.predicate.and(otherPredicate));
    }

    public static class Builder {
        private final List<String> allText = new ArrayList<>();
        private final Set<String> exactMatch = new HashSet<>();
        private final Set<String> nonExactMatch = new LinkedHashSet<>();

        public Builder include(String pattern) {
            allText.add(pattern);
            if (pattern.startsWith("/") || pattern.contains("*") || pattern.contains("?")) {
                nonExactMatch.add(pattern);
            } else {
                exactMatch.add(pattern);
            }
            return this;
        }

        public Builder includeAll(String... patterns) {
            for (String pattern : patterns) {
                include(pattern);
            }
            return this;
        }

        public Builder includeAll(Iterable<String> patterns) {
            for (String pattern : patterns) {
                include(pattern);
            }
            return this;
        }

        public StringMatcher build() {
            if (allText.isEmpty()) {
                return MATCH_NOTHING;
            }

            final String description = describe(allText);
            if (nonExactMatch.contains("*")) {
                return new StringMatcher(description, ALWAYS_TRUE_PREDICATE);
            }
            if (exactMatch.isEmpty()) {
                return new StringMatcher(description, buildAutomataPredicate(nonExactMatch));
            }
            if (nonExactMatch.isEmpty()) {
                return new StringMatcher(description, buildExactMatchPredicate(exactMatch));
            }
            final Predicate<String> predicate = buildExactMatchPredicate(exactMatch).or(buildAutomataPredicate(nonExactMatch));
            return new StringMatcher(description, predicate);
        }

        private static String describe(List<String> strings) {
            if (strings.size() == 1) {
                return strings.get(0);
            }
            final int totalLength = strings.stream().map(String::length).reduce(0, Math::addExact);
            if (totalLength < 250) {
                return Strings.collectionToDelimitedString(strings, "|");
            }
            final int maxItemLength = Math.max(16, 250 / strings.size());
            return strings.stream().map(s -> {
                if (s.length() > maxItemLength) {
                    return Strings.cleanTruncate(s, maxItemLength - 3) + "...";
                } else {
                    return s;
                }
            }).collect(Collectors.joining("|"));
        }

        private static Predicate<String> buildExactMatchPredicate(Set<String> stringValues) {
            if (stringValues.size() == 1) {
                final String singleValue = stringValues.iterator().next();
                return singleValue::equals;
            }
            return stringValues::contains;
        }

        private static Predicate<String> buildAutomataPredicate(Collection<String> patterns) {
            try {
                return Automatons.predicate(patterns);
            } catch (TooComplexToDeterminizeException e) {
                LOGGER.debug("Pattern automaton [{}] is too complex", patterns);
                String description = Strings.collectionToCommaDelimitedString(patterns);
                if (description.length() > 80) {
                    description = Strings.cleanTruncate(description, 80) + "...";
                }
                throw new ElasticsearchSecurityException("The set of patterns [{}] is too complex to evaluate", e, description);
            }
        }
    }
}
