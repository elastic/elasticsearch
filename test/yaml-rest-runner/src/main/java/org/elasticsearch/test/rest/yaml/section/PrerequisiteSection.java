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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.Features;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents a section where prerequisites to run a specific test section or suite are specified. It is possible to specify preconditions
 * as a set of `skip` criteria (the test or suite will be skipped if the specified conditions are met) or `requires` criteria (the test or
 * suite will be run only if the specified conditions are met)
 * Criteria are based on:
 * - the elasticsearch cluster version the tests are running against (deprecated)
 * - the features supported by the elasticsearch cluster version the tests are running against
 * - a specific test runner feature - some runners may not implement the whole set of features
 * - an operating system (full name, including specific Linux distributions) - some OS might show a certain behavior
 */
public class PrerequisiteSection {
    static class PrerequisiteSectionBuilder {
        String skipVersionRange = null;
        String skipReason = null;
        String requiresReason = null;
        List<String> requiredYamlRunnerFeatures = new ArrayList<>();
        List<String> skipOperatingSystems = new ArrayList<>();

        Set<String> skipClusterFeatures = new HashSet<>();
        Set<String> requiredClusterFeatures = new HashSet<>();

        enum XPackRequired {
            NOT_SPECIFIED,
            YES,
            NO,
            MISMATCHED
        }

        XPackRequired xpackRequired = XPackRequired.NOT_SPECIFIED;

        public PrerequisiteSectionBuilder skipIfVersion(String skipVersionRange) {
            this.skipVersionRange = skipVersionRange;
            return this;
        }

        public PrerequisiteSectionBuilder setSkipReason(String skipReason) {
            this.skipReason = skipReason;
            return this;
        }

        public PrerequisiteSectionBuilder setRequiresReason(String requiresReason) {
            this.requiresReason = requiresReason;
            return this;
        }

        public PrerequisiteSectionBuilder requireYamlRunnerFeature(String featureName) {
            requiredYamlRunnerFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder requireXPack() {
            if (xpackRequired == XPackRequired.NO) {
                xpackRequired = XPackRequired.MISMATCHED;
            } else {
                xpackRequired = XPackRequired.YES;
            }
            return this;
        }

        public PrerequisiteSectionBuilder skipIfXPack() {
            if (xpackRequired == XPackRequired.YES) {
                xpackRequired = XPackRequired.MISMATCHED;
            } else {
                xpackRequired = XPackRequired.NO;
            }
            return this;
        }

        public PrerequisiteSectionBuilder skipIfClusterFeature(String featureName) {
            skipClusterFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder requireClusterFeature(String featureName) {
            requiredClusterFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder skipIfOs(String osName) {
            this.skipOperatingSystems.add(osName);
            return this;
        }

        void validate(XContentLocation contentLocation) {
            if ((Strings.hasLength(skipVersionRange) == false)
                && requiredYamlRunnerFeatures.isEmpty()
                && skipOperatingSystems.isEmpty()
                && xpackRequired == XPackRequired.NOT_SPECIFIED
                && requiredClusterFeatures.isEmpty()
                && skipClusterFeatures.isEmpty()) {
                throw new ParsingException(
                    contentLocation,
                    "at least one criteria (version, cluster features, runner features, os) is mandatory within a skip section"
                );
            }
            if (Strings.hasLength(skipVersionRange) && Strings.hasLength(skipReason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip version section");
            }
            if (skipOperatingSystems.isEmpty() == false && Strings.hasLength(skipReason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip os section");
            }
            if (skipClusterFeatures.isEmpty() == false && Strings.hasLength(skipReason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip cluster_features section");
            }
            if (requiredClusterFeatures.isEmpty() == false && Strings.hasLength(requiresReason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within requires cluster_features section");
            }
            // make feature "skip_os" mandatory if os is given, this is a temporary solution until language client tests know about os
            if (skipOperatingSystems.isEmpty() == false && requiredYamlRunnerFeatures.contains("skip_os") == false) {
                throw new ParsingException(contentLocation, "if os is specified, test runner feature [skip_os] must be set");
            }
            if (xpackRequired == XPackRequired.MISMATCHED) {
                throw new ParsingException(contentLocation, "either [xpack] or [no_xpack] can be present, not both");
            }
            if (Sets.haveNonEmptyIntersection(skipClusterFeatures, requiredClusterFeatures)) {
                throw new ParsingException(contentLocation, "a cluster feature can be specified either in [requires] or [skip], not both");
            }
        }

        public PrerequisiteSection build() {
            final List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList = new ArrayList<>();
            final List<Predicate<ClientYamlTestExecutionContext>> requiresCriteriaList;

            // Check if the test runner supports all YAML framework features (see {@link Features}). If not, default to always skip this
            // section.
            if (Features.areAllSupported(requiredYamlRunnerFeatures) == false) {
                requiresCriteriaList = List.of(Prerequisites.FALSE);
            } else {
                requiresCriteriaList = new ArrayList<>();
                if (xpackRequired == XPackRequired.YES) {
                    requiresCriteriaList.add(Prerequisites.hasXPack());
                }
                if (xpackRequired == XPackRequired.NO) {
                    skipCriteriaList.add(Prerequisites.hasXPack());
                }
                if (Strings.hasLength(skipVersionRange)) {
                    skipCriteriaList.add(Prerequisites.skipOnVersionRange(skipVersionRange));
                }
                if (skipOperatingSystems.isEmpty() == false) {
                    skipCriteriaList.add(Prerequisites.skipOnOsList(skipOperatingSystems));
                }
                if (requiredClusterFeatures.isEmpty() == false) {
                    requiresCriteriaList.add(Prerequisites.requireClusterFeatures(requiredClusterFeatures));
                }
                if (skipClusterFeatures.isEmpty() == false) {
                    skipCriteriaList.add(Prerequisites.skipOnClusterFeatures(skipClusterFeatures));
                }
            }
            return new PrerequisiteSection(skipCriteriaList, skipReason, requiresCriteriaList, requiresReason, requiredYamlRunnerFeatures);
        }
    }

    /**
     * Parse a {@link PrerequisiteSection} if the next field is {@code skip}, otherwise returns {@link PrerequisiteSection#EMPTY}.
     */
    public static PrerequisiteSection parseIfNext(XContentParser parser) throws IOException {
        return parseInternal(parser).build();
    }

    private static void maybeAdvanceToNextField(XContentParser parser) throws IOException {
        var token = parser.nextToken();
        if (token != null && token != XContentParser.Token.END_ARRAY) {
            ParserUtils.advanceToFieldName(parser);
        }
    }

    static PrerequisiteSectionBuilder parseInternal(XContentParser parser) throws IOException {
        PrerequisiteSectionBuilder builder = new PrerequisiteSectionBuilder();
        var hasPrerequisiteSection = false;
        var unknownFieldName = false;
        ParserUtils.advanceToFieldName(parser);
        while (unknownFieldName == false) {
            if ("skip".equals(parser.currentName())) {
                parseSkipSection(parser, builder);
                hasPrerequisiteSection = true;
                maybeAdvanceToNextField(parser);
            } else if ("requires".equals(parser.currentName())) {
                parseRequiresSection(parser, builder);
                hasPrerequisiteSection = true;
                maybeAdvanceToNextField(parser);
            } else {
                unknownFieldName = true;
            }
        }
        if (hasPrerequisiteSection) {
            builder.validate(parser.getTokenLocation());
        }
        return builder;
    }

    private static void parseFeatureField(String feature, PrerequisiteSectionBuilder builder) {
        // #31403 introduced YAML test "features" to indicate if the cluster being tested has xpack installed (`xpack`)
        // or if it does *not* have xpack installed (`no_xpack`). These are not test runner features, so now that we have
        // "modular" skip criteria let's separate them. Eventually, these should move to their own skip section.
        if (feature.equals("xpack")) {
            builder.requireXPack();
        } else if (feature.equals("no_xpack")) {
            builder.skipIfXPack();
        } else {
            builder.requireYamlRunnerFeature(feature);
        }
    }

    // package private for tests
    static void parseSkipSection(XContentParser parser, PrerequisiteSectionBuilder builder) throws IOException {
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

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("version".equals(currentFieldName)) {
                    builder.skipIfVersion(parser.text());
                } else if ("reason".equals(currentFieldName)) {
                    builder.setSkipReason(parser.text());
                } else if ("features".equals(currentFieldName)) {
                    parseFeatureField(parser.text(), builder);
                } else if ("os".equals(currentFieldName)) {
                    builder.skipIfOs(parser.text());
                } else if ("cluster_features".equals(currentFieldName)) {
                    builder.skipIfClusterFeature(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "field " + currentFieldName + " not supported within skip section"
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        parseFeatureField(parser.text(), builder);
                    }
                } else if ("os".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.skipIfOs(parser.text());
                    }
                } else if ("cluster_features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.skipIfClusterFeature(parser.text());
                    }
                }
            }
        }
        parser.nextToken();
    }

    static void parseRequiresSection(XContentParser parser, PrerequisiteSectionBuilder builder) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "Expected ["
                    + XContentParser.Token.START_OBJECT
                    + ", found ["
                    + parser.currentToken()
                    + "], the requires section is not properly indented"
            );
        }
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("reason".equals(currentFieldName)) {
                    builder.setRequiresReason(parser.text());
                } else if ("test_runner_features".equals(currentFieldName)) {
                    parseFeatureField(parser.text(), builder);
                } else if ("cluster_features".equals(currentFieldName)) {
                    builder.requireClusterFeature(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "field " + currentFieldName + " not supported within requires section"
                    );
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("test_runner_features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        parseFeatureField(parser.text(), builder);
                    }
                } else if ("cluster_features".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.requireClusterFeature(parser.text());
                    }
                }
            }
        }
        parser.nextToken();
    }

    public static final PrerequisiteSection EMPTY = new PrerequisiteSection();

    private final List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList;
    private final List<Predicate<ClientYamlTestExecutionContext>> requiresCriteriaList;
    private final List<String> yamlRunnerFeatures;
    final String skipReason;
    final String requireReason;

    private PrerequisiteSection() {
        this.skipCriteriaList = new ArrayList<>();
        this.requiresCriteriaList = new ArrayList<>();
        this.yamlRunnerFeatures = new ArrayList<>();
        this.skipReason = null;
        this.requireReason = null;
    }

    PrerequisiteSection(
        List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList,
        String skipReason,
        List<Predicate<ClientYamlTestExecutionContext>> requiresCriteriaList,
        String requireReason,
        List<String> yamlRunnerFeatures
    ) {
        this.skipCriteriaList = skipCriteriaList;
        this.requiresCriteriaList = requiresCriteriaList;
        this.yamlRunnerFeatures = yamlRunnerFeatures;
        this.skipReason = skipReason;
        this.requireReason = requireReason;
    }

    public boolean hasYamlRunnerFeature(String feature) {
        return yamlRunnerFeatures.contains(feature);
    }

    boolean skipCriteriaMet(ClientYamlTestExecutionContext context) {
        return skipCriteriaList.stream().anyMatch(c -> c.test(context));
    }

    boolean requiresCriteriaMet(ClientYamlTestExecutionContext context) {
        return requiresCriteriaList.stream().allMatch(c -> c.test(context));
    }

    public void evaluate(ClientYamlTestExecutionContext context, String testCandidateDescription) {
        if (isEmpty()) {
            return;
        }

        if (requiresCriteriaMet(context) == false) {
            throw new AssumptionViolatedException(buildMessage(testCandidateDescription, false));
        }

        if (skipCriteriaMet(context)) {
            throw new AssumptionViolatedException(buildMessage(testCandidateDescription, true));
        }
    }

    boolean isEmpty() {
        return skipCriteriaList.isEmpty() && requiresCriteriaList.isEmpty() && yamlRunnerFeatures.isEmpty();
    }

    String buildMessage(String description, boolean isSkip) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[").append(description).append("] skipped,");
        var reason = isSkip ? skipReason : requireReason;
        if (Strings.isNullOrEmpty(reason) == false) {
            messageBuilder.append(" reason: [").append(reason).append("]");
        }
        if (yamlRunnerFeatures.isEmpty() == false) {
            messageBuilder.append(" unsupported features ").append(yamlRunnerFeatures);
        }
        return messageBuilder.toString();
    }
}
