/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.section.RestTestSuite;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Parser for a complete test suite (yaml file)
 *
 * Depending on the elasticsearch version the tests are going to run against, a whole test suite might need to get skipped
 * In that case the relevant test sections parsing is entirely skipped
 */
public class RestTestSuiteParser implements RestTestFragmentParser<RestTestSuite> {

    public RestTestSuite parse(String currentVersion, String api, File file) throws IOException, RestTestParseException {

        if (!file.isFile()) {
            throw new IllegalArgumentException(file.getAbsolutePath() + " is not a file");
        }

        XContentParser parser = YamlXContent.yamlXContent.createParser(new FileInputStream(file));
        try {
            String filename = file.getName();
            //remove the file extension
            int i = filename.lastIndexOf('.');
            if (i > 0) {
                filename = filename.substring(0, i);
            }
            RestTestSuiteParseContext testParseContext = new RestTestSuiteParseContext(api, filename, parser, currentVersion);
            return parse(testParseContext);
        } finally {
            parser.close();
        }
    }

    @Override
    public RestTestSuite parse(RestTestSuiteParseContext parseContext) throws IOException, RestTestParseException {
        XContentParser parser = parseContext.parser();

        parser.nextToken();
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;

        RestTestSuite restTestSuite = new RestTestSuite(parseContext.getApi(), parseContext.getSuiteName());

        restTestSuite.setSetupSection(parseContext.parseSetupSection());

        boolean skip = restTestSuite.getSetupSection().getSkipSection().skipVersion(parseContext.getCurrentVersion());

        while(true) {
            //the "---" section separator is not understood by the yaml parser. null is returned, same as when the parser is closed
            //we need to somehow distinguish between a null in the middle of a test ("---")
            // and a null at the end of the file (at least two consecutive null tokens)
            if(parser.currentToken() == null) {
                if (parser.nextToken() == null) {
                    break;
                }
            }

            if (skip) {
                //if there was a skip section, there was a setup section as well, which means that we are sure
                // the current token is at the beginning of a new object
                assert parser.currentToken() == XContentParser.Token.START_OBJECT;
                //we need to be at the beginning of an object to be able to skip children
                parser.skipChildren();
                //after skipChildren we are at the end of the skipped object, need to move on
                parser.nextToken();
            } else {
                restTestSuite.addTestSection(parseContext.parseTestSection());
            }
        }

        return restTestSuite;
    }
}
