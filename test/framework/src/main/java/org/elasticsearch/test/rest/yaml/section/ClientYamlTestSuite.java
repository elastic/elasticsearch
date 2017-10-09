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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Holds a REST test suite loaded from a specific yaml file.
 * Supports a setup section and multiple test sections.
 */
public class ClientYamlTestSuite {
    public static ClientYamlTestSuite parse(String api, Path file) throws IOException {
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

        try (XContentParser parser = YamlXContent.yamlXContent.createParser(ExecutableSection.XCONTENT_REGISTRY,
                Files.newInputStream(file))) {
            return parse(api, filename, parser);
        } catch(Exception e) {
            throw new IOException("Error parsing " + api + "/" + filename, e);
        }
    }

    public static ClientYamlTestSuite parse(String api, String suiteName, XContentParser parser) throws IOException {
        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected token to be START_OBJECT but was "
                + parser.currentToken();

        ClientYamlTestSuite restTestSuite = new ClientYamlTestSuite(api, suiteName);

        restTestSuite.setSetupSection(SetupSection.parseIfNext(parser));
        restTestSuite.setTeardownSection(TeardownSection.parseIfNext(parser));

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
            if (!restTestSuite.addTestSection(testSection)) {
                throw new ParsingException(testSection.getLocation(), "duplicate test section [" + testSection.getName() + "]");
            }
        }

        return restTestSuite;
    }

    private final String api;
    private final String name;

    private SetupSection setupSection;
    private TeardownSection teardownSection;

    private Set<ClientYamlTestSection> testSections = new TreeSet<>();

    public ClientYamlTestSuite(String api, String name) {
        this.api = api;
        this.name = name;
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

    public void setSetupSection(SetupSection setupSection) {
        this.setupSection = setupSection;
    }

    public TeardownSection getTeardownSection() {
        return teardownSection;
    }

    public void setTeardownSection(TeardownSection teardownSection) {
        this.teardownSection = teardownSection;
    }

    /**
     * Adds a {@link org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection} to the REST suite
     * @return true if the test section was not already present, false otherwise
     */
    public boolean addTestSection(ClientYamlTestSection testSection) {
        return this.testSections.add(testSection);
    }

    public List<ClientYamlTestSection> getTestSections() {
        return new ArrayList<>(testSections);
    }
}
