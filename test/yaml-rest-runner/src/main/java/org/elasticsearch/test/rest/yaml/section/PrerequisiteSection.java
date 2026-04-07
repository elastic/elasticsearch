/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.Features;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;

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
    record KnownIssue(String clusterFeature, String fixedBy) {
        private static final Set<String> FIELD_NAMES = Set.of("cluster_feature", "fixed_by");
    }

    record CapabilitiesCheck(String method, String path, String parameters, String capabilities) {
        private static final Set<String> FIELD_NAMES = Set.of("method", "path", "parameters", "capabilities");
    }

    static class PrerequisiteSectionBuilder {
        String skipReason = null;
        String skipVersionRange = null;
        List<String> skipOperatingSystems = new ArrayList<>();
        List<KnownIssue> skipKnownIssues = new ArrayList<>();
        String skipAwaitsFix = null;
        Set<String> skipClusterFeatures = new HashSet<>();
        List<CapabilitiesCheck> skipCapabilities = new ArrayList<>();

        String requiresReason = null;
        List<String> requiredYamlRunnerFeatures = new ArrayList<>();
        Set<String> requiredClusterFeatures = new HashSet<>();
        List<CapabilitiesCheck> requiredCapabilities = new ArrayList<>();

        enum XPackRequired {
            NOT_SPECIFIED,
            YES,
            NO,
            MISMATCHED
        }

        XPackRequired xpackRequired = XPackRequired.NOT_SPECIFIED;

        public PrerequisiteSectionBuilder skipIfAwaitsFix(String bugUrl) {
            this.skipAwaitsFix = bugUrl;
            return this;
        }

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

        public PrerequisiteSectionBuilder skipKnownIssue(KnownIssue knownIssue) {
            skipKnownIssues.add(knownIssue);
            return this;
        }

        public PrerequisiteSectionBuilder skipIfCapabilities(CapabilitiesCheck capabilitiesCheck) {
            skipCapabilities.add(capabilitiesCheck);
            return this;
        }

        public PrerequisiteSectionBuilder requireClusterFeature(String featureName) {
            requiredClusterFeatures.add(featureName);
            return this;
        }

        public PrerequisiteSectionBuilder requireCapabilities(CapabilitiesCheck capabilitiesCheck) {
            requiredCapabilities.add(capabilitiesCheck);
            return this;
        }

        public PrerequisiteSectionBuilder skipIfOs(String osName) {
            this.skipOperatingSystems.add(osName);
            return this;
        }

        void validate(XContentLocation contentLocation) {
            if ((Strings.isEmpty(skipVersionRange))
                && skipOperatingSystems.isEmpty()
                && skipClusterFeatures.isEmpty()
                && skipCapabilities.isEmpty()
                && skipKnownIssues.isEmpty()
                && Strings.isEmpty(skipAwaitsFix)
                && xpackRequired == XPackRequired.NOT_SPECIFIED
                && requiredYamlRunnerFeatures.isEmpty()
                && requiredCapabilities.isEmpty()
                && requiredClusterFeatures.isEmpty()) {
                // TODO separate the validation for requires / skip when dropping parsing of legacy fields, e.g. features in skip
                throw new ParsingException(contentLocation, "at least one predicate is mandatory within a skip or requires section");
            }

            if (Strings.isEmpty(skipReason)
                && (Strings.isEmpty(skipVersionRange)
                    && skipOperatingSystems.isEmpty()
                    && skipClusterFeatures.isEmpty()
                    && skipCapabilities.isEmpty()
                    && skipKnownIssues.isEmpty()) == false) {
                throw new ParsingException(contentLocation, "reason is mandatory within this skip section");
            }

            if (Strings.isEmpty(requiresReason) && ((requiredClusterFeatures.isEmpty() && requiredCapabilities.isEmpty()) == false)) {
                throw new ParsingException(contentLocation, "reason is mandatory within this requires section");
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
            if (Features.areAllSupported(requiredYamlRunnerFeatures) == false) {
                // always skip this section due to missing required test runner features (see {@link Features})
                return new PrerequisiteSection(
                    emptyList(),
                    skipReason,
                    List.of(Prerequisites.FALSE),
                    requiresReason,
                    requiredYamlRunnerFeatures
                );
            }
            if (Strings.hasLength(skipAwaitsFix)) {
                // always skip this section due to a pending fix
                return new PrerequisiteSection(
                    List.of(Prerequisites.TRUE),
                    skipReason,
                    emptyList(),
                    requiresReason,
                    requiredYamlRunnerFeatures
                );
            }

            final List<Predicate<ClientYamlTestExecutionContext>> skipCriteriaList = new ArrayList<>();
            final List<Predicate<ClientYamlTestExecutionContext>> requiresCriteriaList = new ArrayList<>();
            if (xpackRequired == XPackRequired.YES) {
                requiresCriteriaList.add(Prerequisites.hasXPack());
            }
            if (requiredClusterFeatures.isEmpty() == false) {
                requiresCriteriaList.add(Prerequisites.requireClusterFeatures(requiredClusterFeatures));
            }
            if (requiredCapabilities.isEmpty() == false) {
                requiresCriteriaList.add(Prerequisites.requireCapabilities(requiredCapabilities));
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
            if (skipClusterFeatures.isEmpty() == false) {
                skipCriteriaList.add(Prerequisites.skipOnClusterFeatures(skipClusterFeatures));
            }
            if (skipCapabilities.isEmpty() == false) {
                skipCriteriaList.add(Prerequisites.skipCapabilities(skipCapabilities));
            }
            if (skipKnownIssues.isEmpty() == false) {
                skipCriteriaList.add(Prerequisites.skipOnKnownIssue(skipKnownIssues));
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
        requireStartObject("skip", parser.nextToken());

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) continue;

            boolean valid = false;
            if (parser.currentToken().isValue()) {
                valid = switch (parser.currentName()) {
                    case "reason" -> parseString(parser, builder::setSkipReason);
                    case "features" -> parseString(parser, f -> parseFeatureField(f, builder));
                    case "os" -> parseString(parser, builder::skipIfOs);
                    case "cluster_features" -> parseString(parser, builder::skipIfClusterFeature);
                    case "awaits_fix" -> parseString(parser, builder::skipIfAwaitsFix);
                    default -> false;
                };
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                valid = switch (parser.currentName()) {
                    case "features" -> parseStrings(parser, f -> parseFeatureField(f, builder));
                    case "os" -> parseStrings(parser, builder::skipIfOs);
                    case "cluster_features" -> parseStrings(parser, builder::skipIfClusterFeature);
                    case "known_issues" -> parseArray(parser, PrerequisiteSection::parseKnownIssue, builder::skipKnownIssue);
                    case "capabilities" -> parseArray(parser, PrerequisiteSection::parseCapabilities, builder::skipIfCapabilities);
                    default -> false;
                };
            }
            if (valid == false) throwUnexpectedField("skip", parser);
        }
        parser.nextToken();
    }

    private static void throwUnexpectedField(String section, XContentParser parser) throws IOException {
        throw new ParsingException(
            parser.getTokenLocation(),
            Strings.format("field [%s] of type [%s] not supported within %s section", parser.currentName(), parser.currentToken(), section)
        );
    }

    private static void requireStartObject(String section, XContentParser.Token token) throws IOException {
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Expected [%s], found [%s], the %s section is not properly indented",
                    XContentParser.Token.START_OBJECT,
                    token,
                    section
                )
            );
        }
    }

    private static boolean parseString(XContentParser parser, Consumer<String> consumer) throws IOException {
        consumer.accept(parser.text());
        return true;
    }

    private static boolean parseStrings(XContentParser parser, Consumer<String> consumer) throws IOException {
        return parseArray(parser, XContentParser::text, consumer);
    }

    private static <T> boolean parseArray(XContentParser parser, CheckedFunction<XContentParser, T, IOException> item, Consumer<T> consumer)
        throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            consumer.accept(item.apply(parser));
        }
        return true;
    }

    private static KnownIssue parseKnownIssue(XContentParser parser) throws IOException {
        Map<String, String> fields = parser.mapStrings();
        if (fields.keySet().equals(KnownIssue.FIELD_NAMES) == false) {
            throw new ParsingException(
                parser.getTokenLocation(),
                Strings.format("Expected all of %s, but got %s", KnownIssue.FIELD_NAMES, fields.keySet())
            );
        }
        return new KnownIssue(fields.get("cluster_feature"), fields.get("fixed_by"));
    }

    private static CapabilitiesCheck parseCapabilities(XContentParser parser) throws IOException {
        Map<String, Object> fields = parser.map();
        if (CapabilitiesCheck.FIELD_NAMES.containsAll(fields.keySet()) == false) {
            throw new ParsingException(
                parser.getTokenLocation(),
                Strings.format("Expected some of %s, but got %s", CapabilitiesCheck.FIELD_NAMES, fields.keySet())
            );
        }
        Object path = fields.get("path");
        if (path == null) {
            throw new ParsingException(parser.getTokenLocation(), "path is required");
        }

        return new CapabilitiesCheck(
            ensureString(ensureString(fields.getOrDefault("method", "GET"))),
            ensureString(path),
            stringArrayAsParamString("parameters", fields),
            stringArrayAsParamString("capabilities", fields)
        );
    }

    private static String ensureString(Object obj) {
        if (obj instanceof String str) return str;
        throw new IllegalArgumentException("Expected STRING, but got: " + obj);
    }

    private static String stringArrayAsParamString(String name, Map<String, Object> fields) {
        Object value = fields.get(name);
        if (value == null) return null;
        if (value instanceof Collection<?> values) {
            return values.stream().map(PrerequisiteSection::ensureString).collect(joining(","));
        }
        return ensureString(value);
    }

    static void parseRequiresSection(XContentParser parser, PrerequisiteSectionBuilder builder) throws IOException {
        requireStartObject("requires", parser.nextToken());

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) continue;

            boolean valid = false;
            if (parser.currentToken().isValue()) {
                valid = switch (parser.currentName()) {
                    case "reason" -> parseString(parser, builder::setRequiresReason);
                    case "test_runner_features" -> parseString(parser, f -> parseFeatureField(f, builder));
                    case "cluster_features" -> parseString(parser, builder::requireClusterFeature);
                    default -> false;
                };
            } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                valid = switch (parser.currentName()) {
                    case "test_runner_features" -> parseStrings(parser, f -> parseFeatureField(f, builder));
                    case "cluster_features" -> parseStrings(parser, builder::requireClusterFeature);
                    case "capabilities" -> parseArray(parser, PrerequisiteSection::parseCapabilities, builder::requireCapabilities);
                    default -> false;
                };
            }
            if (valid == false) throwUnexpectedField("requires", parser);
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
        this.skipCriteriaList = emptyList();
        this.requiresCriteriaList = emptyList();
        this.yamlRunnerFeatures = emptyList();
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

    boolean hasCapabilitiesCheck() {
        return Stream.concat(skipCriteriaList.stream(), requiresCriteriaList.stream())
            .anyMatch(p -> p instanceof Prerequisites.CapabilitiesPredicate);
    }
}
