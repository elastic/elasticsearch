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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class ClientYamlTestSuite {
    public static ClientYamlTestSuite parse(NamedXContentRegistry executeableSectionRegistry, String api, Path file) throws IOException {
        if (Files.isRegularFile(file) == false) {
            throw new IllegalArgumentException(file.toAbsolutePath() + " is not a file");
        }

        String filename = file.getFileName().toString();
        //remove the file extension
        int i = filename.lastIndexOf('.');
        if (i > 0) {
            filename = filename.substring(0, i);
        }

        //our yaml parser seems to be too tolerant. Each yaml suite must end with \n, otherwise clients tests might break.
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
            if (channel.size() == 0) {
                throw new IllegalArgumentException("test suite file " + file.toString() + " is empty");
            }
            channel.read(bb, channel.size() - 1);
            if (bb.get(0) != 10) {
                throw new IOException("test suite [" + api + "/" + filename + "] doesn't end with line feed (\\n)");
            }
        }

        try (XContentParser parser = YamlXContent.yamlXContent.createParser(executeableSectionRegistry,
            LoggingDeprecationHandler.INSTANCE, Files.newInputStream(file))) {
            return parse(api, filename, parser);
        } catch(Exception e) {
            throw new IOException("Error parsing " + api + "/" + filename, e);
        }
    }

    public static ClientYamlTestSuite parse(String api, String suiteName, XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new XContentParseException(parser.getTokenLocation(),
                    "expected token to be START_OBJECT but was " + parser.currentToken());
        }

        SetupSection setupSection = SetupSection.parseIfNext(parser);
        TeardownSection teardownSection = TeardownSection.parseIfNext(parser);
        Set<ClientYamlTestSection> testSections = new TreeSet<>();
        while(true) {
            //the "---" section separator is not understood by the yaml parser. null is returned, same as when the parser is closed
            //we need to somehow distinguish between a null in the middle of a test ("---")
            // and a null at the end of the file (at least two consecutive null tokens)
            if(parser.currentToken() == null) {
                if (parser.nextToken() == null) {
                    break;
                }
            }
            ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);
            if (testSections.add(testSection) == false) {
                throw new ParsingException(testSection.getLocation(), "duplicate test section [" + testSection.getName() + "]");
            }
        }

        return new ClientYamlTestSuite(api, suiteName, setupSection, teardownSection, new ArrayList<>(testSections));
    }

    private final String api;
    private final String name;
    private final SetupSection setupSection;
    private final TeardownSection teardownSection;
    private final List<ClientYamlTestSection> testSections;

    public ClientYamlTestSuite(String api, String name, SetupSection setupSection, TeardownSection teardownSection,
                        List<ClientYamlTestSection> testSections) {
        this.api = api.replace("\\", "/");  //since api's are sourced from the filesystem normalize backslashes to "/"
        this.name = name;
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

    public SetupSection getSetupSection() {
        return setupSection;
    }

    public TeardownSection getTeardownSection() {
        return teardownSection;
    }

    public void validate() {
        Stream<String> errors = validateExecutableSections(setupSection.getExecutableSections(), null, setupSection, null);
        errors = Stream.concat(errors, validateExecutableSections(teardownSection.getDoSections(), null, null, teardownSection));
        errors = Stream.concat(errors, testSections.stream()
            .flatMap(section -> validateExecutableSections(section.getExecutableSections(), section, setupSection, teardownSection)));
        String errorMessage = errors.collect(Collectors.joining(",\n"));
        if (errorMessage.isEmpty() == false) {
            throw new IllegalArgumentException(getPath() + ":\n" + errorMessage);
        }
    }

    private static Stream<String> validateExecutableSections(List<ExecutableSection> sections,
                                                   ClientYamlTestSection testSection,
                                                   SetupSection setupSection, TeardownSection teardownSection) {

        Stream<String> errors = sections.stream().filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> false == section.getExpectedWarningHeaders().isEmpty())
            .filter(section -> false == hasSkipFeature("warnings", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [do] with a [warnings] section " +
                "without a corresponding [\"skip\": \"features\": \"warnings\"] so runners that do not support the [warnings] " +
                "section can skip the test at line [" + section.getLocation().lineNumber + "]");

        errors = Stream.concat(errors, sections.stream().filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> false == section.getExpectedWarningHeadersRegex()
                .isEmpty())
            .filter(section -> false == hasSkipFeature("warnings_regex", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [do] with a [warnings_regex] section " +
                "without a corresponding [\"skip\": \"features\": \"warnings_regex\"] so runners that do not support the [warnings_regex] "+
                "section can skip the test at line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream().filter(section -> section instanceof DoSection)
                .map(section -> (DoSection) section)
                .filter(section -> false == section.getAllowedWarningHeaders().isEmpty())
                .filter(section -> false == hasSkipFeature("allowed_warnings", testSection, setupSection, teardownSection))
                .map(section -> "attempted to add a [do] with a [allowed_warnings] section " +
                    "without a corresponding [\"skip\": \"features\": \"allowed_warnings\"] so runners that do not " +
                    "support the [allowed_warnings] section can skip the test at line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream().filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> false == section.getAllowedWarningHeadersRegex().isEmpty())
            .filter(section -> false == hasSkipFeature("allowed_warnings_regex", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [do] with a [allowed_warnings_regex] section " +
                "without a corresponding [\"skip\": \"features\": \"allowed_warnings_regex\"] so runners that do not " +
                "support the [allowed_warnings_regex] section can skip the test at line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream().filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> NodeSelector.ANY != section.getApiCallSection().getNodeSelector())
            .filter(section -> false == hasSkipFeature("node_selector", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [do] with a [node_selector] " +
                "section without a corresponding [\"skip\": \"features\": \"node_selector\"] so runners that do not support the " +
                "[node_selector] section can skip the test at line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream()
            .filter(section -> section instanceof ContainsAssertion)
            .filter(section -> false == hasSkipFeature("contains", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [contains] assertion " +
                "without a corresponding [\"skip\": \"features\": \"contains\"] so runners that do not support the " +
                "[contains] assertion can skip the test at line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream().filter(section -> section instanceof DoSection)
            .map(section -> (DoSection) section)
            .filter(section -> false == section.getApiCallSection().getHeaders().isEmpty())
            .filter(section -> false == hasSkipFeature("headers", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [do] with a [headers] section without a corresponding "
                + "[\"skip\": \"features\": \"headers\"] so runners that do not support the [headers] section can skip the test at " +
                "line [" + section.getLocation().lineNumber + "]"));

        errors = Stream.concat(errors, sections.stream()
            .filter(section -> section instanceof CloseToAssertion)
            .filter(section -> false == hasSkipFeature("close_to", testSection, setupSection, teardownSection))
            .map(section -> "attempted to add a [close_to] assertion " +
                "without a corresponding [\"skip\": \"features\": \"close_to\"] so runners that do not support the " +
                "[close_to] assertion can skip the test at line [" + section.getLocation().lineNumber + "]"));

        return errors;
    }

    private static boolean hasSkipFeature(String feature, ClientYamlTestSection testSection,
                                          SetupSection setupSection, TeardownSection teardownSection) {
        return (testSection != null && hasSkipFeature(feature, testSection.getSkipSection())) ||
            (setupSection != null && hasSkipFeature(feature, setupSection.getSkipSection())) ||
            (teardownSection != null && hasSkipFeature(feature, teardownSection.getSkipSection()));
    }

    private static boolean hasSkipFeature(String feature, SkipSection skipSection) {
        return skipSection != null && skipSection.getFeatures().contains(feature);
    }

    public List<ClientYamlTestSection> getTestSections() {
        return testSections;
    }
}
