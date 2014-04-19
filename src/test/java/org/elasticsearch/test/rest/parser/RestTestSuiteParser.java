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
package org.elasticsearch.test.rest.parser;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.TestSection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Parser for a complete test suite (yaml file)
 */
public class RestTestSuiteParser implements RestTestFragmentParser<RestTestSuite> {

    public RestTestSuite parse(String api, File file) throws IOException, RestTestParseException {

        if (!file.isFile()) {
            throw new IllegalArgumentException(file.getAbsolutePath() + " is not a file");
        }

        String filename = file.getName();
        //remove the file extension
        int i = filename.lastIndexOf('.');
        if (i > 0) {
            filename = filename.substring(0, i);
        }

        //our yaml parser seems to be too tolerant. Each yaml suite must end with \n, otherwise clients tests might break.
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.skipBytes((int)randomAccessFile.length() - 1);
            if (randomAccessFile.read() != 10) {
                throw new RestTestParseException("test suite [" + api + "/" + filename + "] doesn't end with line feed (\\n)");
            }
        } finally {
            IOUtils.close(randomAccessFile);
        }

        XContentParser parser = YamlXContent.yamlXContent.createParser(new FileInputStream(file));
        try {
            RestTestSuiteParseContext testParseContext = new RestTestSuiteParseContext(api, filename, parser);
            return parse(testParseContext);
        } catch(Exception e) {
            throw new RestTestParseException("Error parsing " + api + "/" + filename, e);
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

        while(true) {
            //the "---" section separator is not understood by the yaml parser. null is returned, same as when the parser is closed
            //we need to somehow distinguish between a null in the middle of a test ("---")
            // and a null at the end of the file (at least two consecutive null tokens)
            if(parser.currentToken() == null) {
                if (parser.nextToken() == null) {
                    break;
                }
            }

            TestSection testSection = parseContext.parseTestSection();
            if (!restTestSuite.addTestSection(testSection)) {
                throw new RestTestParseException("duplicate test section [" + testSection.getName() + "] found in [" + restTestSuite.getPath() + "]");
            }
        }

        return restTestSuite;
    }
}
