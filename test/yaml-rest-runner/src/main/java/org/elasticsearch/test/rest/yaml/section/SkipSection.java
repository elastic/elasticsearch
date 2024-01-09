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
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.Features;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on:
 * - the elasticsearch version the tests are running against
 * - a specific test feature required that might not be implemented yet by the runner
 * - an operating system (full name, including specific Linux distributions) that might show a certain behavior
 */
public class SkipSection {

    static class PrerequisiteSectionBuilder {
        String version = null;
        String reason = null;
        List<String> requiredYamlRunnerFeatures = new ArrayList<>();
        List<String> operatingSystems = new ArrayList<>();

        Set<String> forbiddenClusterFeatures = new HashSet<>();
        Set<String> requiredClusterFeatures = new HashSet<>();

        enum XPackRequired {
            NOT_SPECIFIED,
            YES,
            NO,
            MISMATCHED
        }

        XPackRequired xpackRequired = XPackRequired.NOT_SPECIFIED;

        public PrerequisiteSectionBuilder skipIfVersion(String version) {
            this.version = version;
            return this;
        }

        public PrerequisiteSectionBuilder setSkipReason(String reason) {
            this.reason = reason;
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
            forbiddenClusterFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder requireClusterFeature(String featureName) {
            requiredClusterFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder skipIfOs(String osName) {
            this.operatingSystems.add(osName);
            return this;
        }

        void validate(XContentLocation contentLocation) {
            if ((Strings.hasLength(version) == false)
                && requiredYamlRunnerFeatures.isEmpty()
                && operatingSystems.isEmpty()
                && xpackRequired == XPackRequired.NOT_SPECIFIED
                && requiredClusterFeatures.isEmpty()
                && forbiddenClusterFeatures.isEmpty()) {
                throw new ParsingException(
                    contentLocation,
                    "at least one criteria (version, cluster features, runner features, os) is mandatory within a skip section"
                );
            }
            if (Strings.hasLength(version) && Strings.hasLength(reason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip version section");
            }
            if (operatingSystems.isEmpty() == false && Strings.hasLength(reason) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within skip version section");
            }
            // make feature "skip_os" mandatory if os is given, this is a temporary solution until language client tests know about os
            if (operatingSystems.isEmpty() == false && requiredYamlRunnerFeatures.contains("skip_os") == false) {
                throw new ParsingException(contentLocation, "if os is specified, feature skip_os must be set");
            }
            if (xpackRequired == XPackRequired.MISMATCHED) {
                throw new ParsingException(contentLocation, "either `xpack` or `no_xpack` can be present, not both");
            }
            if (forbiddenClusterFeatures.stream().anyMatch(x -> requiredClusterFeatures.contains(x))) {
                throw new ParsingException(
                    contentLocation,
                    "skip on a cluster feature can be when it is either present or missing, not both"
                );
            }
        }

        public SkipSection build() {
            final List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList = new ArrayList<>();
            final List<Predicate<ClientYamlTestExecutionContext>> requireCriteriaList;

            // Check if the test runner supports all YAML framework features (see {@link Features}). If not, default to always skip this
            // section.
            if (Features.areAllSupported(requiredYamlRunnerFeatures) == false) {
                requireCriteriaList = List.of(Prerequisites.FALSE);
            } else {
                requireCriteriaList = new ArrayList<>();
                if (xpackRequired == XPackRequired.YES) {
                    requireCriteriaList.add(Prerequisites.hasXPack());
                }
                if (xpackRequired == XPackRequired.NO) {
                    skipCriteriaList.add(Prerequisites.hasXPack());
                }
                if (Strings.hasLength(version)) {
                    skipCriteriaList.add(Prerequisites.skipOnVersionRange(version));
                }
                if (operatingSystems.isEmpty() == false) {
                    skipCriteriaList.add(Prerequisites.skipOnOsList(operatingSystems));
                }
                if (requiredClusterFeatures.isEmpty() == false) {
                    requireCriteriaList.add(Prerequisites.requireClusterFeatures(requiredClusterFeatures));
                }
                if (forbiddenClusterFeatures.isEmpty() == false) {
                    skipCriteriaList.add(Prerequisites.skipOnClusterFeatures(forbiddenClusterFeatures));
                }
            }
            return new SkipSection(skipCriteriaList, requireCriteriaList, requiredYamlRunnerFeatures, reason);
        }
    }

    /**
     * Parse a {@link SkipSection} if the next field is {@code skip}, otherwise returns {@link SkipSection#EMPTY}.
     */
    public static SkipSection parseIfNext(XContentParser parser) throws IOException {
        PrerequisiteSectionBuilder builder = new PrerequisiteSectionBuilder();
        var insidePrerequisiteSection = true;
        while (insidePrerequisiteSection) {
            ParserUtils.advanceToFieldName(parser);
            if ("skip".equals(parser.currentName())) {
                parseSkipSection(parser, builder);
                parser.nextToken();
            } else if ("requires".equals(parser.currentName())) {
                // parseRequireSection(parser, builder);
            } else {
                insidePrerequisiteSection = false;
            }
        }

        return builder.build();
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
                } else if ("cluster_features_present".equals(currentFieldName)) {
                    builder.skipIfClusterFeature(parser.text());
                } else if ("cluster_features_absent".equals(currentFieldName)) {
                    builder.requireClusterFeature(parser.text());
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
                } else if ("cluster_features_present".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.skipIfClusterFeature(parser.text());
                    }
                } else if ("cluster_features_absent".equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        builder.requireClusterFeature(parser.text());
                    }
                }
            }
        }

        parser.nextToken();
        builder.validate(parser.getTokenLocation());
    }

    public static final SkipSection EMPTY = new SkipSection();

    private final List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList;
    private final List<Predicate<ClientYamlTestExecutionContext>> requireCriteriaList;
    private final List<String> yamlRunnerFeatures;
    private final String reason;

    private SkipSection() {
        this.skipCriteriaList = new ArrayList<>();
        this.requireCriteriaList = new ArrayList<>();
        this.yamlRunnerFeatures = new ArrayList<>();
        this.reason = null;
    }

    SkipSection(
        List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList,
        List<Predicate<ClientYamlTestExecutionContext>> requireCriteriaList,
        List<String> yamlRunnerFeatures,
        String reason
    ) {
        this.skipCriteriaList = skipCriteriaList;
        this.requireCriteriaList = requireCriteriaList;
        this.yamlRunnerFeatures = yamlRunnerFeatures;
        this.reason = reason;
    }

    public boolean yamlRunnerHasFeature(String feature) {
        return yamlRunnerFeatures.contains(feature);
    }

    public String getReason() {
        return reason;
    }

    public boolean skip(ClientYamlTestExecutionContext context) {
        if (isEmpty()) {
            return false;
        }

        if (requireCriteriaList.stream().allMatch(c -> c.test(context)) == false) {
            return true;
        }

        return skipCriteriaList.stream().anyMatch(c -> c.test(context));
    }

    public boolean isEmpty() {
        return skipCriteriaList.isEmpty() && requireCriteriaList.isEmpty() && yamlRunnerFeatures.isEmpty();
    }

    public String getSkipMessage(String description) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("[").append(description).append("] skipped,");
        if (reason != null) {
            messageBuilder.append(" reason: [").append(getReason()).append("]");
        }
        if (yamlRunnerFeatures.isEmpty() == false) {
            messageBuilder.append(" unsupported features ").append(yamlRunnerFeatures);
        }
        return messageBuilder.toString();
    }
}
