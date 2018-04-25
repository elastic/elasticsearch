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
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * These tests are not part of {@link ClientYamlSuiteRestApiParserTests} because the tested failures don't allow to consume the whole yaml
 * stream
 */
public class ClientYamlSuiteRestApiParserFailingTests extends ESTestCase {

    public void testDuplicateMethods() throws Exception {
       parseAndExpectFailure("{\n" +
               "  \"ping\": {" +
               "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
               "    \"methods\": [\"PUT\", \"PUT\"]," +
               "    \"url\": {" +
               "      \"path\": \"/\"," +
               "      \"paths\": [\"/\"]," +
               "      \"parts\": {" +
               "      }," +
               "      \"params\": {" +
               "        \"type\" : \"boolean\",\n" +
               "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"" +
               "      }" +
               "    }," +
               "    \"body\": null" +
               "  }" +
               "}", "ping.json", "Found duplicate method [PUT]");
    }

    public void testDuplicatePaths() throws Exception {
        parseAndExpectFailure("{\n" +
                "  \"ping\": {" +
                "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
                "    \"methods\": [\"PUT\"]," +
                "    \"url\": {" +
                "      \"path\": \"/pingone\"," +
                "      \"paths\": [\"/pingone\", \"/pingtwo\", \"/pingtwo\"]," +
                "      \"parts\": {" +
                "      }," +
                "      \"params\": {" +
                "        \"type\" : \"boolean\",\n" +
                "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"" +
                "      }" +
                "    }," +
                "    \"body\": null" +
                "  }" +
                "}", "ping.json", "Found duplicate path [/pingtwo]");
    }

    public void testDuplicateParts() throws Exception {
        assumeFalse("Test only makes sense if XContent parser doesn't have strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());
        parseAndExpectFailure("{\n" +
                "  \"ping\": {" +
                "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
                "    \"methods\": [\"PUT\"]," +
                "    \"url\": {" +
                "      \"path\": \"/\"," +
                "      \"paths\": [\"/\"]," +
                "      \"parts\": {" +
                "        \"index\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"index part\"\n" +
                "        }," +
                "        \"type\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"type part\"\n" +
                "        }," +
                "        \"index\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"index parameter part\"\n" +
                "        }" +
                "      }," +
                "      \"params\": {" +
                "        \"type\" : \"boolean\",\n" +
                "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"" +
                "      }" +
                "    }," +
                "    \"body\": null" +
                "  }" +
                "}", "ping.json", "Found duplicate part [index]");
    }

    public void testDuplicateParams() throws Exception {
        assumeFalse("Test only makes sense if XContent parser doesn't have strict duplicate checks enabled",
            XContent.isStrictDuplicateDetectionEnabled());
        parseAndExpectFailure("{\n" +
                "  \"ping\": {" +
                "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
                "    \"methods\": [\"PUT\"]," +
                "    \"url\": {" +
                "      \"path\": \"/\"," +
                "      \"paths\": [\"/\"]," +
                "      \"parts\": {" +
                "      }," +
                "      \"params\": {" +
                "        \"timeout\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"timeout parameter\"\n" +
                "        }," +
                "        \"refresh\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"refresh parameter\"\n" +
                "        }," +
                "        \"timeout\": {" +
                "          \"type\" : \"string\",\n" +
                "          \"description\" : \"timeout parameter again\"\n" +
                "        }" +
                "      }" +
                "    }," +
                "    \"body\": null" +
                "  }" +
                "}", "ping.json", "Found duplicate param [timeout]");
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParams() throws Exception {
        parseAndExpectFailure(BROKEN_SPEC_PARAMS, "ping.json", "Expected params field in rest api definition to contain an object");
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParts() throws Exception {
        parseAndExpectFailure(BROKEN_SPEC_PARTS, "ping.json", "Expected parts field in rest api definition to contain an object");
    }

    public void testSpecNameMatchesFilename() throws Exception {
        parseAndExpectFailure("{\"ping\":{}}", "not_matching.json", "API [ping] should have the same name as its file [not_matching.json]");
    }

    private void parseAndExpectFailure(String brokenJson, String location, String expectedErrorMessage) throws Exception {
        XContentParser parser = createParser(YamlXContent.yamlXContent, brokenJson);
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> restApiParser.parse(location, parser));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    // see params section is broken, an inside param is missing
    private static final String BROKEN_SPEC_PARAMS = "{\n" +
            "  \"ping\": {" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
            "    \"methods\": [\"HEAD\"]," +
            "    \"url\": {" +
            "      \"path\": \"/\"," +
            "      \"paths\": [\"/\"]," +
            "      \"parts\": {" +
            "      }," +
            "      \"params\": {" +
            "        \"type\" : \"boolean\",\n" +
            "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"\n" +
            "      }" +
            "    }," +
            "    \"body\": null" +
            "  }" +
            "}";

    // see parts section is broken, an inside param is missing
    private static final String BROKEN_SPEC_PARTS = "{\n" +
            "  \"ping\": {" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
            "    \"methods\": [\"HEAD\"]," +
            "    \"url\": {" +
            "      \"path\": \"/\"," +
            "      \"paths\": [\"/\"]," +
            "      \"parts\": {" +
            "          \"type\" : \"boolean\",\n" +
            "      }," +
            "      \"params\": {\n" +
            "        \"ignore_unavailable\": {\n" +
            "          \"type\" : \"boolean\",\n" +
            "          \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"\n" +
            "        } \n" +
            "    }," +
            "    \"body\": null" +
            "  }" +
            "}";

}
