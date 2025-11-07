/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.ElasticsearchStatusException;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;

/**
 * Tech Preview.
 * Resolves a single entry _alias for a cross-project request specifying a project_routing.
 * We currently only support a single entry routing containing either a specific name, a prefix, a suffix, or a match-all (*).
 */
public class CrossProjectRoutingResolver implements ProjectRoutingResolver {
    private static final String ALIAS = "_alias:";
    private static final int ALIAS_LENGTH = ALIAS.length();
    private static final String ALIAS_MATCH_ALL = ALIAS + "*";
    private static final String ALIAS_MATCH_ORIGIN = ALIAS + ORIGIN;

    /**
     * Initially, we only support the "*" wildcard.
     * https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-query-string-query
     */
    private static final Set<Character> UNSUPPORTED_CHARACTERS = Set.of(
        '+',
        '-',
        '=',
        '&',
        '|',
        '>',
        '<',
        '!',
        '(',
        ')',
        '{',
        '}',
        '[',
        ']',
        '^',
        '"',
        '~',
        '?',
        ':',
        '\\',
        '/'
    );

    /**
     * Filters the specified TargetProjects based on the provided project routing string
     * @param projectRouting the project_routing specified in the request object
     * @param targetProjects The target projects to be filtered
     * @return A new TargetProjects instance containing only the projects that match the project routing.
     *  @throws ElasticsearchStatusException if the projectRouting is null, empty, does not start with "_alias:", contains more than one
     *                                       entry, or contains an '*' in the middle of a string.
     */
    @Override
    public TargetProjects resolve(String projectRouting, TargetProjects targetProjects) {
        assert targetProjects != TargetProjects.LOCAL_ONLY_FOR_CPS_DISABLED;
        if (targetProjects.isEmpty()) {
            return TargetProjects.EMPTY;
        }

        if (projectRouting == null || projectRouting.isEmpty() || ALIAS_MATCH_ALL.equalsIgnoreCase(projectRouting)) {
            return targetProjects;
        }

        final var originProject = targetProjects.originProject();
        assert originProject != null : "origin project must not be null";
        // TODO: some of the assertions such as alias and non-overlapping could be enforced in TargetProjects constructor
        assert originProject.projectAlias().equalsIgnoreCase(ORIGIN) == false : "origin project alias must not be " + ORIGIN;

        if (ALIAS_MATCH_ORIGIN.equalsIgnoreCase(projectRouting)) {
            return new TargetProjects(originProject, List.of());
        }

        final var candidateProjects = targetProjects.linkedProjects();
        assert candidateProjects != null : "candidate projects must not be null";

        var candidateProjectStream = candidateProjects.stream().peek(candidateProject -> {
            assert candidateProject.projectAlias().equalsIgnoreCase(ORIGIN) == false : "project alias must not be " + ORIGIN;
        }).filter(candidateProject -> {
            assert candidateProject.equals(originProject) == false : "origin project must not be in the candidateProjects list";
            return candidateProject.equals(originProject) == false; // assertions are disabled in prod, instead we should filter this out
        });

        validateProjectRouting(projectRouting);

        var matchesSpecifiedRoute = createRoutingEntryFilter(projectRouting);
        return new TargetProjects(
            matchesSpecifiedRoute.test(originProject) ? originProject : null,
            candidateProjectStream.filter(matchesSpecifiedRoute).toList()
        );
    }

    private static void validateProjectRouting(String projectRouting) {
        var startsWithAlias = startsWithIgnoreCase(ALIAS, projectRouting);
        if (startsWithAlias && projectRouting.length() == ALIAS_LENGTH) {
            throw new ElasticsearchStatusException("project_routing expression [{}] cannot be empty", BAD_REQUEST, projectRouting);
        }
        if ((startsWithAlias == false) && projectRouting.contains(":")) {
            throw new ElasticsearchStatusException(
                "Unsupported tag [{}] in project_routing expression [{}]. Supported tags [_alias].",
                BAD_REQUEST,
                projectRouting.substring(0, projectRouting.indexOf(":")),
                projectRouting
            );
        }
        if (startsWithAlias == false) {
            throw new ElasticsearchStatusException(
                "project_routing [{}] must start with the prefix [_alias:]",
                BAD_REQUEST,
                projectRouting
            );
        }
    }

    private static Predicate<ProjectRoutingInfo> createRoutingEntryFilter(String projectRouting) {
        // we're using index pointers and directly accessing the internal character array rather than using higher abstraction
        // methods like String.split or creating multiple substrings. we don't expect a lot of linked projects or long project routing
        // expressions, but this is expected to run on every search request so we're opting to avoid creating multiple objects.
        // plus we plan to replace this all soon anyway...
        var matchPrefix = projectRouting.charAt(projectRouting.length() - 1) == '*';
        var matchSuffix = projectRouting.charAt(ALIAS_LENGTH) == '*';

        int foundAsterix = -1;
        int startIndex = matchSuffix ? ALIAS_LENGTH + 1 : ALIAS_LENGTH;
        int endIndex = matchPrefix ? projectRouting.length() - 1 : projectRouting.length();

        for (int i = startIndex; i < endIndex; ++i) {
            var nextChar = projectRouting.charAt(i);

            // verify that there are no whitespaces, unsupported characters,
            // or more complex asterisk expressions (*pro*_2 is unsupported, pro*_2, pro*, and *project_2 are all supported)
            if (Character.isWhitespace(nextChar)
                || UNSUPPORTED_CHARACTERS.contains(nextChar)
                || (nextChar == '*' && (foundAsterix >= 0 || matchPrefix || matchSuffix))) {
                throw new ElasticsearchStatusException(
                    "Unsupported project_routing expression [{}]. "
                        + "Tech Preview only supports project routing via a single project alias or wildcard alias expression",
                    BAD_REQUEST,
                    projectRouting.substring(ALIAS_LENGTH)
                );
            }

            if (nextChar == '*') {
                foundAsterix = i;
            }
        }

        if (foundAsterix >= 0) {
            var prefix = projectRouting.substring(startIndex, foundAsterix);
            var suffix = projectRouting.substring(foundAsterix + 1, endIndex);
            return possibleRoute -> startsWithIgnoreCase(prefix, possibleRoute.projectAlias())
                && endsWithIgnoreCase(suffix, possibleRoute.projectAlias());
        }

        var routingEntry = projectRouting.substring(startIndex, endIndex);
        if (matchPrefix && matchSuffix) {
            return possibleRoute -> containsIgnoreCase(routingEntry, possibleRoute.projectAlias());
        } else if (matchPrefix) {
            return possibleRoute -> startsWithIgnoreCase(routingEntry, possibleRoute.projectAlias());
        } else if (matchSuffix) {
            return possibleRoute -> endsWithIgnoreCase(routingEntry, possibleRoute.projectAlias());
        } else {
            return possibleRoute -> possibleRoute.projectAlias().equalsIgnoreCase(routingEntry);
        }
    }

    private static boolean startsWithIgnoreCase(String prefix, String str) {
        if (prefix == null || str == null) {
            return false;
        }
        if (str.startsWith(prefix)) {
            return true;
        }
        if (str.length() < prefix.length()) {
            return false;
        }
        if (str.length() == prefix.length() && str.equalsIgnoreCase(prefix)) {
            return true;
        }
        return str.substring(0, prefix.length()).equalsIgnoreCase(prefix);
    }

    private static boolean endsWithIgnoreCase(String suffix, String str) {
        if (suffix == null || str == null) {
            return false;
        }
        if (str.endsWith(suffix)) {
            return true;
        }
        if (str.length() < suffix.length()) {
            return false;
        }
        if (str.length() == suffix.length() && str.equalsIgnoreCase(suffix)) {
            return true;
        }
        return str.substring(str.length() - suffix.length()).equalsIgnoreCase(suffix);
    }

    private static boolean containsIgnoreCase(String substring, String str) {
        if (substring == null || str == null) {
            return false;
        }
        if (str.contains(substring)) {
            return true;
        }
        if (str.length() < substring.length()) {
            return false;
        }
        if (str.length() == substring.length() && str.equalsIgnoreCase(substring)) {
            return true;
        }
        var substringLength = substring.length();
        return IntStream.range(0, str.length() - substringLength).anyMatch(i -> str.regionMatches(true, i, substring, 0, substringLength));
    }
}
