/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class ClientYamlTestSuite {
    public static ClientYamlTestSuite parse(NamedXContentRegistry executeableSectionRegistry, String api, Path file) throws IOException {
        if (!Files.isRegularFile(file)) {
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

    ClientYamlTestSuite(String api, String name, SetupSection setupSection, TeardownSection teardownSection,
                        List<ClientYamlTestSection> testSections) {
        this.api = api;
        this.name = name;
        this.setupSection = setupSection;
        this.teardownSection = teardownSection;
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
        validateExecutableSections(this, setupSection.getExecutableSections(), null, setupSection, null);
        validateExecutableSections(this, teardownSection.getDoSections(), null, null, teardownSection);
        for (ClientYamlTestSection testSection : testSections) {
            validateExecutableSections(this, testSection.getExecutableSections(), testSection, setupSection, teardownSection);
        }
    }

    private static void validateExecutableSections(ClientYamlTestSuite yamlTestSuite, List<ExecutableSection> sections,
                                                   ClientYamlTestSection testSection,
                                                   SetupSection setupSection, TeardownSection teardownSection) {
        for (ExecutableSection section : sections) {
            if (section instanceof DoSection) {
                DoSection doSection = (DoSection) section;
                if (false == doSection.getExpectedWarningHeaders().isEmpty()
                    && false == hasSkipFeature("warnings", testSection, setupSection, teardownSection)) {
                    throw new IllegalArgumentException(yamlTestSuite.getPath() + ": attempted to add a [do] with a [warnings] section " +
                        "without a corresponding [\"skip\": \"features\": \"warnings\"] so runners that do not support the [warnings] " +
                        "section can skip the test at line [" + doSection.getLocation().lineNumber + "]");
                }
                if (NodeSelector.ANY != doSection.getApiCallSection().getNodeSelector()
                    && false == hasSkipFeature("node_selector", testSection, setupSection, teardownSection)) {
                    throw new IllegalArgumentException(yamlTestSuite.getPath() + ": attempted to add a [do] with a [node_selector] " +
                        "section without a corresponding [\"skip\": \"features\": \"node_selector\"] so runners that do not support the " +
                        "[node_selector] section can skip the test at line [" + doSection.getLocation().lineNumber + "]");
                }
            }
            if (section instanceof ContainsAssertion
                && false == hasSkipFeature("contains", testSection, setupSection, teardownSection)) {
                throw new IllegalArgumentException(yamlTestSuite.getPath() + ": attempted to add a [contains] assertion " +
                    "without a corresponding [\"skip\": \"features\": \"contains\"] so runners that do not support the " +
                    "[contains] assertion can skip the test at line [" + section.getLocation().lineNumber + "]");
            }
        }
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
