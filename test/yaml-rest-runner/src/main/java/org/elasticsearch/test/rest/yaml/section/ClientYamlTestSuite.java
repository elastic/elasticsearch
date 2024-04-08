/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.test.rest.yaml.ParameterizableYamlXContentParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class ClientYamlTestSuite {
    public static ClientYamlTestSuite parse(NamedXContentRegistry executeableSectionRegistry, String api, Path file, Map<String, ?> params)
        throws IOException {
        if (Files.isRegularFile(file) == false) {
            throw new IllegalArgumentException(file.toAbsolutePath() + " is not a file");
        }

        String filename = file.getFileName().toString();
        // remove the file extension
        int i = filename.lastIndexOf('.');
        if (i > 0) {
            filename = filename.substring(0, i);
        }

        // our yaml parser seems to be too tolerant. Each yaml suite must end with \n, otherwise clients tests might break.
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
            if (channel.size() == 0) {
                throw new IllegalArgumentException("test suite file " + file.toString() + " is empty");
            }
            Channels.readFromFileChannel(channel, channel.size() - 1, bb);
            if (bb.get(0) != 10) {
                throw new IOException("test suite [" + api + "/" + filename + "] doesn't end with line feed (\\n)");
            }
        }

        try (
            XContentParser parser = params.isEmpty()
                ? YamlXContent.yamlXContent.createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(executeableSectionRegistry),
                    Files.newInputStream(file)
                )
                : new ParameterizableYamlXContentParser(
                    YamlXContent.yamlXContent.createParser(
                        XContentParserConfiguration.EMPTY.withRegistry(executeableSectionRegistry),
                        Files.newInputStream(file)
                    ),
                    params
                )
        ) {
            return parse(api, filename, Optional.of(file), parser);
        } catch (Exception e) {
            throw new IOException("Error parsing " + api + "/" + filename, e);
        }
    }

    public static ClientYamlTestSuite parse(String api, String suiteName, Optional<Path> file, XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(
                parser.getTokenLocation(),
                "expected token to be START_OBJECT but was " + parser.currentToken()
            );
        }

        SetupSection setupSection = SetupSection.parseIfNext(parser);
        TeardownSection teardownSection = TeardownSection.parseIfNext(parser);
        Set<ClientYamlTestSection> testSections = new TreeSet<>();
        while (true) {
            // the "---" section separator is not understood by the yaml parser. null is returned, same as when the parser is closed
            // we need to somehow distinguish between a null in the middle of a test ("---")
            // and a null at the end of the file (at least two consecutive null tokens)
            if (parser.currentToken() == null) {
                if (parser.nextToken() == null) {
                    break;
                }
            }
            ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);
            if (testSections.add(testSection) == false) {
                throw new ParsingException(testSection.getLocation(), "duplicate test section [" + testSection.getName() + "]");
            }
        }

        return new ClientYamlTestSuite(api, suiteName, file, setupSection, teardownSection, new ArrayList<>(testSections));
    }

    public static ClientYamlTestSuite parse(NamedXContentRegistry xcontentRegistry, String api, Path filePath) throws IOException {
        return parse(xcontentRegistry, api, filePath, Collections.emptyMap());
    }

    private final String api;
    private final String name;
    private final Optional<Path> file;
    private final SetupSection setupSection;
    private final TeardownSection teardownSection;
    private final List<ClientYamlTestSection> testSections;

    public ClientYamlTestSuite(
        String api,
        String name,
        Optional<Path> file,
        SetupSection setupSection,
        TeardownSection teardownSection,
        List<ClientYamlTestSection> testSections
    ) {
        this.api = api.replace("\\", "/");  // since api's are sourced from the filesystem normalize backslashes to "/"
        this.name = name;
        this.file = file;
        this.setupSection = Objects.requireNonNull(setupSection, "setup section cannot be null");
        this.teardownSection = Objects.requireNonNull(teardownSection, "teardown section cannot be null");
        this.testSections = Collections.unmodifiableList(testSections);
    }

    public String getApi() {
        return api;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return api + "/" + name;
    }

    public Optional<Path> getFile() {
        return file;
    }

    public SetupSection getSetupSection() {
        return setupSection;
    }

    public TeardownSection getTeardownSection() {
        return teardownSection;
    }

    public void validate() {
        Stream<String> errors = validateExecutableSections(setupSection.getExecutableSections(), null, setupSection, null);
        errors = Stream.concat(errors, validateExecutableSections(teardownSection.getDoSections(), null, null, teardownSection));
        errors = Stream.concat(
            errors,
            testSections.stream()
                .flatMap(section -> validateExecutableSections(section.getExecutableSections(), section, setupSection, teardownSection))
        );
        String errorMessage = errors.collect(Collectors.joining(",\n"));
        if (errorMessage.isEmpty() == false) {
            throw new IllegalArgumentException(getPath() + ":\n" + errorMessage);
        }
    }

    private static Stream<String> validateExecutableSections(
        List<ExecutableSection> sections,
        ClientYamlTestSection testSection,
        SetupSection setupSection,
        TeardownSection teardownSection
    ) {

        Stream<String> errors = sections.stream()
            .filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> false == section.getExpectedWarningHeaders().isEmpty())
            .filter(section -> false == hasYamlRunnerFeature("warnings", testSection, setupSection, teardownSection))
            .map(section -> String.format(Locale.ROOT, """
                attempted to add a [do] with a [warnings] section without a corresponding ["requires": "test_runner_features": "warnings"] \
                so runners that do not support the [warnings] section can skip the test at line [%d]\
                """, section.getLocation().lineNumber()));

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> false == section.getExpectedWarningHeadersRegex().isEmpty())
                .filter(section -> false == hasYamlRunnerFeature("warnings_regex", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [do] with a [warnings_regex] section without a corresponding \
                    ["requires": "test_runner_features": "warnings_regex"] so runners that do not support the [warnings_regex] \
                    section can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> false == section.getAllowedWarningHeaders().isEmpty())
                .filter(section -> false == hasYamlRunnerFeature("allowed_warnings", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [do] with a [allowed_warnings] section without a corresponding \
                    ["requires": "test_runner_features": "allowed_warnings"] so runners that do not support the [allowed_warnings] \
                    section can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> false == section.getAllowedWarningHeadersRegex().isEmpty())
                .filter(section -> false == hasYamlRunnerFeature("allowed_warnings_regex", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [do] with a [allowed_warnings_regex] section without a corresponding \
                    ["requires": "test_runner_features": "allowed_warnings_regex"] so runners that do not support the \
                    [allowed_warnings_regex] section can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> NodeSelector.ANY != section.getApiCallSection().getNodeSelector())
                .filter(section -> false == hasYamlRunnerFeature("node_selector", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [do] with a [node_selector] section without a corresponding \
                    ["requires": "test_runner_features": "node_selector"] so runners that do not support the [node_selector] section \
                    can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof ContainsAssertion)
                .filter(section -> false == hasYamlRunnerFeature("contains", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [contains] assertion without a corresponding ["requires": "test_runner_features": "contains"] \
                    so runners that do not support the [contains] assertion can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> false == section.getApiCallSection().getHeaders().isEmpty())
                .filter(section -> false == hasYamlRunnerFeature("headers", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [do] with a [headers] section without a corresponding \
                    ["requires": "test_runner_features": "headers"] so runners that do not support the [headers] section \
                    can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof CloseToAssertion)
                .filter(section -> false == hasYamlRunnerFeature("close_to", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add a [close_to] assertion without a corresponding ["requires": "test_runner_features": "close_to"] \
                    so runners that do not support the [close_to] assertion can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        errors = Stream.concat(
            errors,
            sections.stream()
                .filter(section -> section instanceof IsAfterAssertion)
                .filter(section -> false == hasYamlRunnerFeature("is_after", testSection, setupSection, teardownSection))
                .map(section -> String.format(Locale.ROOT, """
                    attempted to add an [is_after] assertion without a corresponding ["requires": "test_runner_features": "is_after"] \
                    so runners that do not support the [is_after] assertion can skip the test at line [%d]\
                    """, section.getLocation().lineNumber()))
        );

        return errors;
    }

    private static boolean hasYamlRunnerFeature(
        String feature,
        ClientYamlTestSection testSection,
        SetupSection setupSection,
        TeardownSection teardownSection
    ) {
        return (testSection != null && hasYamlRunnerFeature(feature, testSection.getPrerequisiteSection()))
            || (setupSection != null && hasYamlRunnerFeature(feature, setupSection.getPrerequisiteSection()))
            || (teardownSection != null && hasYamlRunnerFeature(feature, teardownSection.getPrerequisiteSection()));
    }

    private static boolean hasYamlRunnerFeature(String feature, PrerequisiteSection prerequisiteSection) {
        return prerequisiteSection != null && prerequisiteSection.hasYamlRunnerFeature(feature);
    }

    public List<ClientYamlTestSection> getTestSections() {
        return testSections;
    }
}
