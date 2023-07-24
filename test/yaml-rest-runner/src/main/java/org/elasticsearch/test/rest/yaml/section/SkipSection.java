/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.rest.yaml.Features;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on:
 * - the elasticsearch version the tests are running against
 * - a specific test feature required that might not be implemented yet by the runner
 * - an operating system (full name, including specific Linux distributions) that might show a certain behavior
 */
public class SkipSection {
    /**
     * Parse a {@link SkipSection} if the next field is {@code skip}, otherwise returns {@link SkipSection#EMPTY}.
     */
    public static SkipSection parseIfNext(XContentParser parser) throws IOException {
        ParserUtils.advanceToFieldName(parser);

        if ("skip".equals(parser.currentName())) {
            SkipSection section = parse(parser);
            parser.nextToken();
            return section;
        }

        return EMPTY;
    }

    public static SkipSection parse(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "Expected ["
                    + XContentParser.Token.START_OBJECT
                    + ", found ["
                    + parser.currentToken()
                    + "], the skip section is not properly indented"
            );
        }
        String currentFieldName = null;
        XContentParser.Token token;
        String version = null;
        String reason = null;
        List<String> features = new ArrayList<>();
        List<String> operatingSystems = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    version = parser.text();
                } else if ("reason".equals(currentFieldName)) {
                    reason = parser.text();
                } else if ("features".equals(currentFieldName)) {
                    features.add(parser.text());
                } else if ("os".equals(currentFieldName)) {
                    operatingSystems.add(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "field " + currentFieldName + " not supported within skip section"
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        features.add(parser.text());
                    }
                } else if ("os".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        operatingSystems.add(parser.text());
                    }
                }
            }
        }

        parser.nextToken();

        if ((Strings.hasLength(version) == false) && features.isEmpty() && operatingSystems.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "version, features or os is mandatory within skip section");
        }
        if (Strings.hasLength(version) && Strings.hasLength(reason) == false) {
            throw new ParsingException(parser.getTokenLocation(), "reason is mandatory within skip version section");
        }
        if (operatingSystems.isEmpty() == false && Strings.hasLength(reason) == false) {
            throw new ParsingException(parser.getTokenLocation(), "reason is mandatory within skip version section");
        }
        // make feature "skip_os" mandatory if os is given, this is a temporary solution until language client tests know about os
        if (operatingSystems.isEmpty() == false && features.contains("skip_os") == false) {
            throw new ParsingException(parser.getTokenLocation(), "if os is specified, feature skip_os must be set");
        }
        return new SkipSection(version, features, operatingSystems, reason);
    }

    public static final SkipSection EMPTY = new SkipSection();

    private final List<VersionRange> versionRanges;
    private final List<String> features;
    private final List<String> operatingSystems;
    private final String reason;

    private SkipSection() {
        this.versionRanges = new ArrayList<>();
        this.features = new ArrayList<>();
        this.operatingSystems = new ArrayList<>();
        this.reason = null;
    }

    public SkipSection(String versionRange, List<String> features, List<String> operatingSystems, String reason) {
        assert features != null;
        this.versionRanges = parseVersionRanges(versionRange);
        assert versionRanges.isEmpty() == false;
        this.features = features;
        this.operatingSystems = operatingSystems;
        this.reason = reason;
    }

    public Version getLowerVersion() {
        return versionRanges.get(0).lower();
    }

    public Version getUpperVersion() {
        return versionRanges.get(versionRanges.size() - 1).upper();
    }

    public List<String> getFeatures() {
        return features;
    }

    public List<String> getOperatingSystems() {
        return operatingSystems;
    }

    public String getReason() {
        return reason;
    }

    public boolean skip(Version currentVersion) {
        if (isEmpty()) {
            return false;
        }
        boolean skip = versionRanges.stream().anyMatch(range -> range.contains(currentVersion));
        return skip || Features.areAllSupported(features) == false;
    }

    public boolean skip(String os) {
        return this.operatingSystems.contains(os);
    }

    public boolean isVersionCheck() {
        return features.isEmpty() && operatingSystems.isEmpty();
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }

    static List<VersionRange> parseVersionRanges(String rawRanges) {
        if (rawRanges == null) {
            return Collections.singletonList(new VersionRange(null, null));
        }
        String[] ranges = rawRanges.split(",");
        List<VersionRange> versionRanges = new ArrayList<>();
        for (String rawRange : ranges) {
            if (rawRange.trim().equals("all")) {
                return Collections.singletonList(new VersionRange(VersionUtils.getFirstVersion(), Version.CURRENT));
            }
            String[] skipVersions = rawRange.split("-", -1);
            if (skipVersions.length > 2) {
                throw new IllegalArgumentException("version range malformed: " + rawRanges);
            }

            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            VersionRange versionRange = new VersionRange(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
            versionRanges.add(versionRange);
        }
        return versionRanges;
    }

    public String getSkipMessage(String description) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[").append(description).append("] skipped,");
        if (reason != null) {
            messageBuilder.append(" reason: [").append(getReason()).append("]");
        }
        if (features.isEmpty() == false) {
            messageBuilder.append(" unsupported features ").append(getFeatures());
        }
        return messageBuilder.toString();
    }
}
