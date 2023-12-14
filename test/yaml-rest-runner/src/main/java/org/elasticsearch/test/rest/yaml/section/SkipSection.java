/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.rest.yaml.Features;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on:
 * - the elasticsearch version the tests are running against
 * - a specific test feature required that might not be implemented yet by the runner
 * - an operating system (full name, including specific Linux distributions) that might show a certain behavior
 */
public class SkipSection {

    static class SkipSectionBuilder {
        String version = null;
        String reason = null;
        List<String> testFeatures = new ArrayList<>();
        List<String> operatingSystems = new ArrayList<>();

        public SkipSectionBuilder withVersion(String version) {
            this.version = version;
            return this;
        }

        public SkipSectionBuilder withReason(String reason) {
            this.reason = reason;
            return this;
        }

        public SkipSectionBuilder withTestFeature(String featureName) {
            this.testFeatures.add(featureName);
            return this;
        }

        public SkipSectionBuilder withOs(String osName) {
            this.operatingSystems.add(osName);
            return this;
        }

        void validate(XContentLocation contentLocation) {
            if ((Strings.hasLength(version) == false) && testFeatures.isEmpty() && operatingSystems.isEmpty()) {
                throw new ParsingException(
                    contentLocation,
                    "at least one criteria (version, test features, os) is mandatory within a skip section"
                );
            }
            if (Strings.hasLength(version) && Strings.hasLength(reason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip version section");
            }
            if (operatingSystems.isEmpty() == false && Strings.hasLength(reason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip version section");
            }
            // make feature "skip_os" mandatory if os is given, this is a temporary solution until language client tests know about os
            if (operatingSystems.isEmpty() == false && testFeatures.contains("skip_os") == false) {
                throw new ParsingException(contentLocation, "if os is specified, feature skip_os must be set");
            }
        }

        public SkipSection build() {
            List<SkipCriteria> skipCriteriaList = new ArrayList<>();
            if (Strings.hasLength(version)) {
                skipCriteriaList.add(new VersionSkipCriteria(version));
            }
            if (operatingSystems.isEmpty() == false) {
                skipCriteriaList.add(new OsSkipCriteria(operatingSystems));
            }
            return new SkipSection(skipCriteriaList, testFeatures, reason);
        }
    }

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
        return parseInternal(parser).build();
    }

    // package private for tests
    static SkipSectionBuilder parseInternal(XContentParser parser) throws IOException {
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

        var builder = new SkipSectionBuilder();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    builder.withVersion(parser.text());
                } else if ("reason".equals(currentFieldName)) {
                    builder.withReason(parser.text());
                } else if ("features".equals(currentFieldName)) {
                    builder.withTestFeature(parser.text());
                } else if ("os".equals(currentFieldName)) {
                    builder.withOs(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "field " + currentFieldName + " not supported within skip section"
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.withTestFeature(parser.text());
                    }
                } else if ("os".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.withOs(parser.text());
                    }
                }
            }
        }

        parser.nextToken();
        builder.validate(parser.getTokenLocation());
        return builder;
    }

    public static final SkipSection EMPTY = new SkipSection();

    private final List<SkipCriteria> skipCriteriaList;
    private final List<String> testFeatures;
    private final String reason;

    private SkipSection() {
        this.skipCriteriaList = new ArrayList<>();
        this.testFeatures = new ArrayList<>();
        this.reason = null;
    }

    SkipSection(List<SkipCriteria> skipCriteriaList, List<String> testFeatures, String reason) {
        this.skipCriteriaList = skipCriteriaList;
        this.testFeatures = testFeatures;
        this.reason = reason;
    }

    public List<String> getFeatures() {
        return testFeatures;
    }

    public String getReason() {
        return reason;
    }

    public boolean skip(SkipSectionContext context) {
        if (isEmpty()) {
            return false;
        }
        if (Features.areAllSupported(testFeatures) == false) {
            return true;
        }
        return skipCriteriaList.stream().anyMatch(c -> c.skip(context));
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }

    public String getSkipMessage(String description) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[").append(description).append("] skipped,");
        if (reason != null) {
            messageBuilder.append(" reason: [").append(getReason()).append("]");
        }
        if (testFeatures.isEmpty() == false) {
            messageBuilder.append(" unsupported features ").append(getFeatures());
        }
        return messageBuilder.toString();
    }
}
