/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Parsed cluster setting mapping projects to isolated tier names per tier.
 * Parsed from JSON (cluster setting value) shaped as:
 * {@code [{"project":"<id>","tiers":{"index":"<name>","search":"<name>"}}, ...]}.
 */
public final class IsolatedProjects {

    /** Isolated tier names are used in Kubernetes resource names in the control plane. */
    public static final int MAX_ISOLATED_TIER_NAME_LENGTH = 15;

    public static final IsolatedProjects EMPTY = new IsolatedProjects(Map.of());

    private final Map<ProjectId, TierIsolationNames> projectIdToTierIsolationNamesMap;

    private IsolatedProjects(Map<ProjectId, TierIsolationNames> projectIdToTierIsolationNamesMap) {
        this.projectIdToTierIsolationNamesMap = projectIdToTierIsolationNamesMap;
    }

    public Optional<String> isolatedTierName(ProjectId projectId, IsolationShardTier isolationShardTier) {
        TierIsolationNames tierIsolationNames = projectIdToTierIsolationNamesMap.get(projectId);
        if (tierIsolationNames == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(switch (isolationShardTier) {
            case INDEX -> tierIsolationNames.indexIsolatedTierName();
            case SEARCH -> tierIsolationNames.searchIsolatedTierName();
        });
    }

    public static IsolatedProjects parse(String rawSettingValue) {
        if (rawSettingValue == null || rawSettingValue.isBlank()) {
            return EMPTY;
        }
        try (
            XContentParser xContentParser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, rawSettingValue)
        ) {
            xContentParser.nextToken();
            return parse(xContentParser);
        } catch (IOException ioException) {
            throw new IllegalArgumentException("failed to parse [" + rawSettingValue + "]", ioException);
        }
    }

    private static IsolatedProjects parse(XContentParser xContentParser) throws IOException {
        XContentParserUtils.ensureExpectedToken(Token.START_ARRAY, xContentParser.currentToken(), xContentParser);
        Map<ProjectId, TierIsolationNames> mergedProjectIdToTierNames = new LinkedHashMap<>();
        Token arrayElementToken;
        while ((arrayElementToken = xContentParser.nextToken()) != Token.END_ARRAY) {
            XContentParserUtils.ensureExpectedToken(Token.START_OBJECT, arrayElementToken, xContentParser);
            String projectIdRawString = null;
            TierIsolationNames tierIsolationNames = null;
            while (xContentParser.nextToken() != Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(Token.FIELD_NAME, xContentParser.currentToken(), xContentParser);
                String entryFieldName = xContentParser.currentName();
                xContentParser.nextToken();
                switch (entryFieldName) {
                    case "project" -> projectIdRawString = xContentParser.text();
                    case "tiers" -> tierIsolationNames = parseTiersObject(xContentParser);
                    default -> throw new IllegalArgumentException("unknown field [" + entryFieldName + "] in isolated_projects entry");
                }
            }
            if (Strings.isNullOrEmpty(projectIdRawString)) {
                throw new IllegalArgumentException("missing required field [project] in isolated_projects entry");
            }
            if (tierIsolationNames == null) {
                throw new IllegalArgumentException("missing required field [tiers] for project [" + projectIdRawString + "]");
            }
            ProjectId parsedProjectId = ProjectId.fromId(projectIdRawString);
            mergedProjectIdToTierNames.merge(parsedProjectId, tierIsolationNames, TierIsolationNames::mergeWith);
        }
        return mergedProjectIdToTierNames.isEmpty() ? EMPTY : new IsolatedProjects(Map.copyOf(mergedProjectIdToTierNames));
    }

    private static TierIsolationNames parseTiersObject(XContentParser xContentParser) throws IOException {
        XContentParserUtils.ensureExpectedToken(Token.START_OBJECT, xContentParser.currentToken(), xContentParser);
        String indexIsolatedTierName = null;
        String searchIsolatedTierName = null;
        while (xContentParser.nextToken() != Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(Token.FIELD_NAME, xContentParser.currentToken(), xContentParser);
            String tierConfigurationFieldName = xContentParser.currentName();
            xContentParser.nextToken();
            switch (tierConfigurationFieldName) {
                case "index" -> indexIsolatedTierName = validateIsolatedTierName(xContentParser.text());
                case "search" -> searchIsolatedTierName = validateIsolatedTierName(xContentParser.text());
                default -> throw new IllegalArgumentException("unknown field [" + tierConfigurationFieldName + "] under [tiers]");
            }
        }
        return new TierIsolationNames(indexIsolatedTierName, searchIsolatedTierName);
    }

    private static String validateIsolatedTierName(String candidateIsolatedTierName) {
        if (candidateIsolatedTierName == null || candidateIsolatedTierName.isEmpty()) {
            throw new IllegalArgumentException("isolated tier name cannot be empty");
        }
        if (candidateIsolatedTierName.length() > MAX_ISOLATED_TIER_NAME_LENGTH) {
            throw new IllegalArgumentException(
                "isolated tier name [" + candidateIsolatedTierName + "] exceeds max length [" + MAX_ISOLATED_TIER_NAME_LENGTH + "]"
            );
        }
        for (int charIndex = 0; charIndex < candidateIsolatedTierName.length(); charIndex++) {
            char tierNameCharacter = candidateIsolatedTierName.charAt(charIndex);
            boolean isLegalDnsLabelCharacter = (tierNameCharacter >= 'a' && tierNameCharacter <= 'z')
                || (tierNameCharacter >= '0' && tierNameCharacter <= '9')
                || tierNameCharacter == '-';
            if (isLegalDnsLabelCharacter == false) {
                throw new IllegalArgumentException(
                    "isolated tier name [" + candidateIsolatedTierName + "] must contain only [a-z0-9-] (DNS label characters)"
                );
            }
        }
        if (candidateIsolatedTierName.startsWith("-") || candidateIsolatedTierName.endsWith("-")) {
            throw new IllegalArgumentException("isolated tier name [" + candidateIsolatedTierName + "] must not start or end with '-'");
        }
        return candidateIsolatedTierName;
    }

    /**
     * Optional index and search isolated tier names for one project (either may be null).
     */
    public record TierIsolationNames(String indexIsolatedTierName, String searchIsolatedTierName) {
        static TierIsolationNames mergeWith(TierIsolationNames firstTierNames, TierIsolationNames secondTierNames) {
            return new TierIsolationNames(
                secondTierNames.indexIsolatedTierName != null
                    ? secondTierNames.indexIsolatedTierName
                    : firstTierNames.indexIsolatedTierName,
                secondTierNames.searchIsolatedTierName != null
                    ? secondTierNames.searchIsolatedTierName
                    : firstTierNames.searchIsolatedTierName
            );
        }
    }
}
