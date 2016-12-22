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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.collect.Tuple.tuple;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Represents a do section:
 *
 *   - do:
 *      catch:      missing
 *      headers:
 *          Authorization: Basic user:pass
 *          Content-Type: application/json
 *      warnings:
 *          - Stuff is deprecated, yo
 *          - Don't use deprecated stuff
 *          - Please, stop. It hurts.
 *      update:
 *          index:  test_1
 *          type:   test
 *          id:     1
 *          body:   { doc: { foo: bar } }
 *
 */
public class DoSection implements ExecutableSection {
    public static DoSection parse(XContentParser parser) throws IOException {
        String currentFieldName = null;
        XContentParser.Token token;

        DoSection doSection = new DoSection(parser.getTokenLocation());
        ApiCallSection apiCallSection = null;
        Map<String, String> headers = new HashMap<>();
        List<String> expectedWarnings = new ArrayList<>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("catch".equals(currentFieldName)) {
                    doSection.setCatch(parser.text());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("warnings".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) == XContentParser.Token.VALUE_STRING) {
                        expectedWarnings.add(parser.text());
                    }
                    if (token != XContentParser.Token.END_ARRAY) {
                        throw new ParsingException(parser.getTokenLocation(), "[warnings] must be a string array but saw [" + token + "]");
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "unknown array [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("headers".equals(currentFieldName)) {
                    String headerName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            headerName = parser.currentName();
                        } else if (token.isValue()) {
                            headers.put(headerName, parser.text());
                        }
                    }
                } else if (currentFieldName != null) { // must be part of API call then
                    apiCallSection = new ApiCallSection(currentFieldName);
                    String paramName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            paramName = parser.currentName();
                        } else if (token.isValue()) {
                            if ("body".equals(paramName)) {
                                String body = parser.text();
                                XContentType bodyContentType = XContentFactory.xContentType(body);
                                XContentParser bodyParser = XContentFactory.xContent(bodyContentType).createParser(
                                        NamedXContentRegistry.EMPTY, body);
                                //multiple bodies are supported e.g. in case of bulk provided as a whole string
                                while(bodyParser.nextToken() != null) {
                                    apiCallSection.addBody(bodyParser.mapOrdered());
                                }
                            } else {
                                apiCallSection.addParam(paramName, parser.text());
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("body".equals(paramName)) {
                                apiCallSection.addBody(parser.mapOrdered());
                            }
                        }
                    }
                }
            }
        }
        try {
            if (apiCallSection == null) {
                throw new IllegalArgumentException("client call section is mandatory within a do section");
            }
            if (headers.isEmpty() == false) {
                apiCallSection.addHeaders(headers);
            }
            doSection.setApiCallSection(apiCallSection);
            doSection.setExpectedWarningHeaders(unmodifiableList(expectedWarnings));
        } finally {
            parser.nextToken();
        }
        return doSection;
    }


    private static final Logger logger = Loggers.getLogger(DoSection.class);

    private final XContentLocation location;
    private String catchParam;
    private ApiCallSection apiCallSection;
    private List<String> expectedWarningHeaders = emptyList();

    public DoSection(XContentLocation location) {
        this.location = location;
    }

    public String getCatch() {
        return catchParam;
    }

    public void setCatch(String catchParam) {
        this.catchParam = catchParam;
    }

    public ApiCallSection getApiCallSection() {
        return apiCallSection;
    }

    public void setApiCallSection(ApiCallSection apiCallSection) {
        this.apiCallSection = apiCallSection;
    }

    /**
     * Warning headers that we expect from this response. If the headers don't match exactly this request is considered to have failed.
     * Defaults to emptyList.
     */
    public List<String> getExpectedWarningHeaders() {
        return expectedWarningHeaders;
    }

    /**
     * Set the warning headers that we expect from this response. If the headers don't match exactly this request is considered to have
     * failed. Defaults to emptyList.
     */
    public void setExpectedWarningHeaders(List<String> expectedWarningHeaders) {
        this.expectedWarningHeaders = expectedWarningHeaders;
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {

        if ("param".equals(catchParam)) {
            //client should throw validation error before sending request
            //lets just return without doing anything as we don't have any client to test here
            logger.info("found [catch: param], no request sent");
            return;
        }

        try {
            ClientYamlTestResponse response = executionContext.callApi(apiCallSection.getApi(), apiCallSection.getParams(),
                    apiCallSection.getBodies(), apiCallSection.getHeaders());
            if (Strings.hasLength(catchParam)) {
                String catchStatusCode;
                if (catches.containsKey(catchParam)) {
                    catchStatusCode = catches.get(catchParam).v1();
                } else if (catchParam.startsWith("/") && catchParam.endsWith("/")) {
                    catchStatusCode = "4xx|5xx";
                } else {
                    throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
                }
                fail(formatStatusCodeMessage(response, catchStatusCode));
            }
            checkWarningHeaders(response.getWarningHeaders());
        } catch(ClientYamlTestResponseException e) {
            ClientYamlTestResponse restTestResponse = e.getRestTestResponse();
            if (!Strings.hasLength(catchParam)) {
                fail(formatStatusCodeMessage(restTestResponse, "2xx"));
            } else if (catches.containsKey(catchParam)) {
                assertStatusCode(restTestResponse);
            } else if (catchParam.length() > 2 && catchParam.startsWith("/") && catchParam.endsWith("/")) {
                //the text of the error message matches regular expression
                assertThat(formatStatusCodeMessage(restTestResponse, "4xx|5xx"),
                        e.getResponseException().getResponse().getStatusLine().getStatusCode(), greaterThanOrEqualTo(400));
                Object error = executionContext.response("error");
                assertThat("error was expected in the response", error, notNullValue());
                //remove delimiters from regex
                String regex = catchParam.substring(1, catchParam.length() - 1);
                assertThat("the error message was expected to match the provided regex but didn't",
                        error.toString(), matches(regex));
            } else {
                throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
            }
        }
    }

    /**
     * Check that the response contains only the warning headers that we expect.
     */
    void checkWarningHeaders(List<String> warningHeaders) {
        StringBuilder failureMessage = null;
        // LinkedHashSet so that missing expected warnings come back in a predictable order which is nice for testing
        Set<String> expected = new LinkedHashSet<>(expectedWarningHeaders);
        for (String header : warningHeaders) {
            if (expected.remove(header)) {
                // Was expected, all good.
                continue;
            }
            if (failureMessage == null) {
                failureMessage = new StringBuilder("got unexpected warning headers [");
            }
            failureMessage.append('\n').append(header);
        }
        if (false == expected.isEmpty()) {
            if (failureMessage == null) {
                failureMessage = new StringBuilder();
            } else {
                failureMessage.append("\n] ");
            }
            failureMessage.append("didn't get expected warning headers [");
            for (String header : expected) {
                failureMessage.append('\n').append(header);
            }
        }
        if (failureMessage != null) {
            fail(failureMessage + "\n]");
        }
    }

    private void assertStatusCode(ClientYamlTestResponse restTestResponse) {
        Tuple<String, org.hamcrest.Matcher<Integer>> stringMatcherTuple = catches.get(catchParam);
        assertThat(formatStatusCodeMessage(restTestResponse, stringMatcherTuple.v1()),
                restTestResponse.getStatusCode(), stringMatcherTuple.v2());
    }

    private String formatStatusCodeMessage(ClientYamlTestResponse restTestResponse, String expected) {
        String api = apiCallSection.getApi();
        if ("raw".equals(api)) {
            api += "[method=" + apiCallSection.getParams().get("method") + " path=" + apiCallSection.getParams().get("path") + "]";
        }
        return "expected [" + expected + "] status code but api [" + api + "] returned [" + restTestResponse.getStatusCode() +
                " " + restTestResponse.getReasonPhrase() + "] [" + restTestResponse.getBodyAsString() + "]";
    }

    private static Map<String, Tuple<String, org.hamcrest.Matcher<Integer>>> catches = new HashMap<>();

    static {
        catches.put("unauthorized", tuple("401", equalTo(401)));
        catches.put("forbidden", tuple("403", equalTo(403)));
        catches.put("missing", tuple("404", equalTo(404)));
        catches.put("request_timeout", tuple("408", equalTo(408)));
        catches.put("conflict", tuple("409", equalTo(409)));
        catches.put("unavailable", tuple("503", equalTo(503)));
        catches.put("request", tuple("4xx|5xx", allOf(greaterThanOrEqualTo(400),
                not(equalTo(401)),
                not(equalTo(403)),
                not(equalTo(404)),
                not(equalTo(408)),
                not(equalTo(409)))));
    }
}
