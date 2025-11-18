/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.action.util;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for tracking the set of Ids returned from some
 * function a satisfy the required Ids as defined by an
 * expression that may contain wildcards.
 *
 * For example, given a set of Ids ["foo-1", "foo-2", "bar-1", bar-2"]:
 * <ul>
 *     <li>The expression foo* would be satisfied by foo-1 and foo-2</li>
 *     <li>The expression bar-1 would be satisfied by bar-1</li>
 *     <li>The expression bar-1,car-1 would leave car-1 unmatched</li>
 *     <li>The expression * would be satisfied by anything or nothing depending on the
 *     value of {@code allowNoMatchForWildcards}</li>
 * </ul>
 */
public final class ExpandedIdsMatcher {

    /**
     * Split {@code expression} into tokens separated by a ','
     *
     * @param expression Expression containing zero or more ','s
     * @return Array of tokens
     */
    public static String[] tokenizeExpression(String expression) {
        return Strings.tokenizeToStringArray(expression, ",");
    }

    private final List<IdMatcher> allMatchers;
    private final List<IdMatcher> requiredMatches;
    private final boolean onlyExact;

    /**
     * Generate the list of required matches from the expressions in {@code tokens}
     * and initialize.
     *
     * @param tokens List of expressions that may be wildcards or full Ids
     * @param allowNoMatchForWildcards If true then it is not required for wildcard
     *                                 expressions to match an Id meaning they are
     *                                 not returned in the list of required matches
     */
    public ExpandedIdsMatcher(String[] tokens, boolean allowNoMatchForWildcards) {
        requiredMatches = new LinkedList<>();
        List<IdMatcher> allMatchers = new ArrayList<>();

        if (Strings.isAllOrWildcard(tokens)) {
            // if allowNoJobForWildcards == true then any number
            // of jobs with any id is ok. Therefore no matches
            // are required

            IdMatcher matcher = new WildcardMatcher("*");
            this.allMatchers = Collections.singletonList(matcher);
            if (allowNoMatchForWildcards == false) {
                // require something, anything to match
                requiredMatches.add(matcher);
            }
            onlyExact = false;
            return;
        }

        boolean atLeastOneWildcard = false;

        if (allowNoMatchForWildcards) {
            // matches are not required for wildcards but
            // specific job Ids are
            for (String token : tokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    allMatchers.add(new WildcardMatcher(token));
                    atLeastOneWildcard = true;
                } else {
                    IdMatcher matcher = new EqualsIdMatcher(token);
                    allMatchers.add(matcher);
                    requiredMatches.add(matcher);
                }
            }
        } else {
            // Matches are required for wildcards
            for (String token : tokens) {
                if (Regex.isSimpleMatchPattern(token)) {
                    IdMatcher matcher = new WildcardMatcher(token);
                    allMatchers.add(matcher);
                    requiredMatches.add(matcher);
                    atLeastOneWildcard = true;
                } else {
                    IdMatcher matcher = new EqualsIdMatcher(token);
                    allMatchers.add(matcher);
                    requiredMatches.add(matcher);
                }
            }
        }
        onlyExact = atLeastOneWildcard == false;
        this.allMatchers = Collections.unmodifiableList(allMatchers);
    }

    /**
     * Generate the list of required matches from the {@code expression}
     * and initialize.
     *
     * @param expression Expression that will be tokenized into a set of wildcards or full Ids
     * @param allowNoMatchForWildcards If true then it is not required for wildcard
     *                                 expressions to match an Id meaning they are
     *                                 not returned in the list of required matches
     */
    public ExpandedIdsMatcher(String expression, boolean allowNoMatchForWildcards) {
        this(tokenizeExpression(expression), allowNoMatchForWildcards);
    }

    /**
     * Test whether an ID matches any of the expressions.
     * Unlike {@link #filterMatchedIds} this does not modify the state of
     * the matcher.
     * @param id ID to test.
     * @return Does the ID match one or more of the patterns in the expression?
     */
    public boolean idMatches(String id) {
        return allMatchers.stream().anyMatch(idMatcher -> idMatcher.matches(id));
    }

    /**
     * For each {@code requiredMatchers} check there is an element
     * present in {@code ids} that matches. Once a match is made the
     * matcher is removed from {@code requiredMatchers}.
     */
    public void filterMatchedIds(Collection<String> ids) {
        for (String id : ids) {
            Iterator<IdMatcher> itr = requiredMatches.iterator();
            if (itr.hasNext() == false) {
                break;
            }
            while (itr.hasNext()) {
                if (itr.next().matches(id)) {
                    itr.remove();
                }
            }
        }
    }

    public boolean hasUnmatchedIds() {
        return requiredMatches.isEmpty() == false;
    }

    public List<String> unmatchedIds() {
        return requiredMatches.stream().map(IdMatcher::getId).collect(Collectors.toList());
    }

    public String unmatchedIdsString() {
        return requiredMatches.stream().map(IdMatcher::getId).collect(Collectors.joining(","));
    }

    /**
     * Whether ids are based on exact matchers or at least one contains a wildcard.
     *
     * @return true if only exact matches, false if at least one id contains a wildcard
     */
    public boolean isOnlyExact() {
        return onlyExact;
    }

    /**
     * A simple matcher with one purpose to test whether an id
     * matches a expression that may contain wildcards.
     * Use the {@link #idMatches(String)} function to
     * test if the given id is matched by any of the matchers.
     *
     * Unlike {@link ExpandedIdsMatcher} there is no
     * allowNoMatchForWildcards logic and the matchers
     * are not be removed once they have been matched.
     */
    public static class SimpleIdsMatcher {

        private final List<IdMatcher> matchers;

        public SimpleIdsMatcher(String[] tokens) {

            if (Strings.isAllOrWildcard(tokens)) {
                matchers = Collections.singletonList(new WildcardMatcher("*"));
                return;
            }

            matchers = Arrays.stream(tokens)
                .map(token -> Regex.isSimpleMatchPattern(token) ? new WildcardMatcher(token) : new EqualsIdMatcher(token))
                .collect(Collectors.toList());
        }

        public SimpleIdsMatcher(String expression) {
            this(tokenizeExpression(expression));
        }

        /**
         * Do any of the matchers match {@code id}?
         *
         * @param id Id to test
         * @return True if the given id is matched by any of the matchers
         */
        public boolean idMatches(String id) {
            return matchers.stream().anyMatch(idMatcher -> idMatcher.matches(id));
        }
    }

    private abstract static class IdMatcher {
        protected final String id;

        IdMatcher(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public abstract boolean matches(String jobId);
    }

    private static class EqualsIdMatcher extends IdMatcher {
        EqualsIdMatcher(String id) {
            super(id);
        }

        @Override
        public boolean matches(String id) {
            return this.id.equals(id);
        }
    }

    private static class WildcardMatcher extends IdMatcher {
        WildcardMatcher(String id) {
            super(id);
        }

        @Override
        public boolean matches(String id) {
            return Regex.simpleMatch(this.id, id);
        }
    }
}
