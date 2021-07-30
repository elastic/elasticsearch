/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.ParsingException;
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
       parseAndExpectParsingException("{\n" +
               "  \"ping\": {" +
               "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
               "    \"stability\": \"stable\",\n" +
               "    \"visibility\": \"public\",\n" +
               "    \"url\": {" +
               "      \"paths\": [{\"path\":\"/\", \"parts\": {}, \"methods\": [\"PUT\", \"PUT\"]}]," +
               "      \"params\": {" +
               "        \"type\" : \"boolean\",\n" +
               "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"" +
               "      }" +
               "    }," +
               "    \"body\": null" +
               "  }" +
               "}", "ping.json", "ping API: found duplicate method [PUT]");
    }

    public void testDuplicatePaths() throws Exception {
        parseAndExpectIllegalArgumentException("{\n" +
                "  \"ping\": {" +
                "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
                "    \"stability\": \"stable\",\n" +
                "    \"visibility\": \"public\",\n" +
                "    \"url\": {" +
                "      \"paths\": [" +
                "         {\"path\":\"/pingtwo\", \"methods\": [\"PUT\"]}, " + "{\"path\":\"/pingtwo\", \"methods\": [\"PUT\"]}]," +
                "      \"params\": {" +
                "        \"type\" : \"boolean\",\n" +
                "        \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"" +
                "      }" +
                "    }," +
                "    \"body\": null" +
                "  }" +
                "}", "ping.json", "ping API: found duplicate path [/pingtwo]");
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParams() throws Exception {
        parseAndExpectParsingException(BROKEN_SPEC_PARAMS, "ping.json",
            "ping API: expected [params] field in rest api definition to contain an object");
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParts() throws Exception {
        parseAndExpectParsingException(BROKEN_SPEC_PARTS, "ping.json",
            "ping API: expected [parts] field in rest api definition to contain an object");
    }

    public void testSpecNameMatchesFilename() throws Exception {
        parseAndExpectIllegalArgumentException("{\"ping\":{}}", "not_matching.json", "API [ping] should have " +
            "the same name as its file [not_matching.json]");
    }

    private void parseAndExpectParsingException(String brokenJson, String location, String expectedErrorMessage) throws Exception {
        XContentParser parser = createParser(YamlXContent.yamlXContent, brokenJson);
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();

        ParsingException e = expectThrows(ParsingException.class, () -> restApiParser.parse(location, parser));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    private void parseAndExpectIllegalArgumentException(String brokenJson, String location, String expectedErrorMessage) throws Exception {
        XContentParser parser = createParser(YamlXContent.yamlXContent, brokenJson);
        ClientYamlSuiteRestApiParser restApiParser = new ClientYamlSuiteRestApiParser();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> restApiParser.parse(location, parser));
        assertThat(e.getMessage(), containsString(expectedErrorMessage));
    }

    // see params section is broken, an inside param is missing
    private static final String BROKEN_SPEC_PARAMS = "{\n" +
            "  \"ping\": {" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
            "    \"stability\": \"stable\",\n" +
            "    \"visibility\": \"public\",\n" +
            "    \"url\": {" +
            "      \"paths\": [{\"path\": \"path\", \"methods\": [\"HEAD\"]}]" +
            "    }," +
            "    \"params\": {" +
            "      \"type\" : \"boolean\",\n" +
            "      \"description\" : \"Whether specified concrete indices should be ignored when unavailable (missing or closed)\"\n" +
            "    }," +
            "    \"body\": null" +
            "  }" +
            "}";

    // see parts section is broken, an inside param is missing
    private static final String BROKEN_SPEC_PARTS = "{\n" +
            "  \"ping\": {" +
            "    \"documentation\": \"http://www.elasticsearch.org/guide/\"," +
            "    \"stability\": \"stable\",\n" +
            "    \"visibility\": \"public\",\n" +
            "    \"url\": {" +
            "      \"paths\": [{ \"path\":\"/\", \"parts\": { \"type\":\"boolean\",}}]," +
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
