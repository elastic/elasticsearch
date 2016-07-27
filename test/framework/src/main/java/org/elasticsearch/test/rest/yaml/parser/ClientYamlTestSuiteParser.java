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
package org.elasticsearch.test.rest.yaml.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Parser for a complete test suite (yaml file)
 */
public class ClientYamlTestSuiteParser implements ClientYamlTestFragmentParser<ClientYamlTestSuite> {

    public ClientYamlTestSuite parse(String api, Path file) throws IOException, ClientYamlTestParseException {

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
            channel.read(bb, channel.size() - 1);
            if (bb.get(0) != 10) {
                throw new ClientYamlTestParseException("test suite [" + api + "/" + filename + "] doesn't end with line feed (\\n)");
            }
        }

        try (XContentParser parser = YamlXContent.yamlXContent.createParser(Files.newInputStream(file))) {
            ClientYamlTestSuiteParseContext testParseContext = new ClientYamlTestSuiteParseContext(api, filename, parser);
            return parse(testParseContext);
        } catch(Exception e) {
            throw new ClientYamlTestParseException("Error parsing " + api + "/" + filename, e);
        }
    }

    @Override
    public ClientYamlTestSuite parse(ClientYamlTestSuiteParseContext parseContext) throws IOException, ClientYamlTestParseException {
        XContentParser parser = parseContext.parser();

        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "expected token to be START_OBJECT but was "
                + parser.currentToken();

        ClientYamlTestSuite restTestSuite = new ClientYamlTestSuite(parseContext.getApi(), parseContext.getSuiteName());

        restTestSuite.setSetupSection(parseContext.parseSetupSection());
        restTestSuite.setTeardownSection(parseContext.parseTeardownSection());

        while(true) {
            //the "---" section separator is not understood by the yaml parser. null is returned, same as when the parser is closed
            //we need to somehow distinguish between a null in the middle of a test ("---")
            // and a null at the end of the file (at least two consecutive null tokens)
            if(parser.currentToken() == null) {
                if (parser.nextToken() == null) {
                    break;
                }
            }

            ClientYamlTestSection testSection = parseContext.parseTestSection();
            if (!restTestSuite.addTestSection(testSection)) {
                throw new ClientYamlTestParseException("duplicate test section [" + testSection.getName() + "] found in ["
                        + restTestSuite.getPath() + "]");
            }
        }

        return restTestSuite;
    }
}
