/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml.restspec;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import static org.hamcrest.Matchers.containsString;

/**
 * These tests are not part of {@link ClientYamlSuiteRestApiParserTests} because the tested failures don't allow to consume the whole yaml
 * stream
 */
public class ClientYamlSuiteRestApiParserFailingTests extends ESTestCase {

    public void testDuplicateMethods() throws Exception {
        parseAndExpectParsingException("""
            {
              "ping": {
                "documentation": "http://www.elasticsearch.org/guide/",
                "stability": "stable",
                "visibility": "public",
                "url": {
                  "paths": [
                    {
                      "path": "/",
                      "parts": {},
                      "methods": [
                        "PUT",
                        "PUT"
                      ]
                    }
                  ],
                  "params": {
                    "type": "boolean",
                    "description": "Whether specified concrete indices should be ignored when unavailable (missing or closed)"
                  }
                },
                "body": null
              }
            }""", "ping.json", "ping API: found duplicate method [PUT]");
    }

    public void testDuplicatePaths() throws Exception {
        parseAndExpectIllegalArgumentException("""
            {
              "ping": {
                "documentation": "http://www.elasticsearch.org/guide/",
                "stability": "stable",
                "visibility": "public",
                "url": {
                  "paths": [
                    {
                      "path": "/pingtwo",
                      "methods": [
                        "PUT"
                      ]
                    },
                    {
                      "path": "/pingtwo",
                      "methods": [
                        "PUT"
                      ]
                    }
                  ],
                  "params": {
                    "type": "boolean",
                    "description": "Whether specified concrete indices should be ignored when unavailable (missing or closed)"
                  }
                },
                "body": null
              }
            }""", "ping.json", "ping API: found duplicate path [/pingtwo]");
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParams() throws Exception {
        parseAndExpectParsingException(
            BROKEN_SPEC_PARAMS,
            "ping.json",
            "ping API: expected [params] field in rest api definition to contain an object"
        );
    }

    public void testBrokenSpecShouldThrowUsefulExceptionWhenParsingFailsOnParts() throws Exception {
        parseAndExpectParsingException(
            BROKEN_SPEC_PARTS,
            "ping.json",
            "ping API: expected [parts] field in rest api definition to contain an object"
        );
    }

    public void testSpecNameMatchesFilename() throws Exception {
        parseAndExpectIllegalArgumentException(
            "{\"ping\":{}}",
            "not_matching.json",
            "API [ping] should have " + "the same name as its file [not_matching.json]"
        );
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
    private static final String BROKEN_SPEC_PARAMS = """
        {
          "ping": {
            "documentation": "http://www.elasticsearch.org/guide/",
            "stability": "stable",
            "visibility": "public",
            "url": {
              "paths": [
                {
                  "path": "path",
                  "methods": [
                    "HEAD"
                  ]
                }
              ]
            },
            "params": {
              "type": "boolean",
              "description": "Whether specified concrete indices should be ignored when unavailable (missing or closed)"
            },
            "body": null
          }
        }""";

    // see parts section is broken, an inside param is missing
    private static final String BROKEN_SPEC_PARTS = """
        {
          "ping": {
            "documentation": "http://www.elasticsearch.org/guide/",
            "stability": "stable",
            "visibility": "public",
            "url": {
              "paths": [{ "path":"/", "parts": { "type":"boolean",}}],
              "params": {
                "ignore_unavailable": {
                  "type" : "boolean",
                  "description" : "Whether specified concrete indices should be ignored when unavailable (missing or closed)"
                }
              },
              "body": null
            }
          }
        """;
}
