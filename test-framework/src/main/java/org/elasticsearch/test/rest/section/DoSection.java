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
package org.elasticsearch.test.rest.section;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.rest.RestTestExecutionContext;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
 *      update:
 *          index:  test_1
 *          type:   test
 *          id:     1
 *          body:   { doc: { foo: bar } }
 *
 */
public class DoSection implements ExecutableSection {

    private static final ESLogger logger = Loggers.getLogger(DoSection.class);

    private String catchParam;
    private ApiCallSection apiCallSection;

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

    @Override
    public void execute(RestTestExecutionContext executionContext) throws IOException {

        if ("param".equals(catchParam)) {
            //client should throw validation error before sending request
            //lets just return without doing anything as we don't have any client to test here
            logger.info("found [catch: param], no request sent");
            return;
        }

        try {
            RestResponse restResponse = executionContext.callApi(apiCallSection.getApi(), apiCallSection.getParams(), apiCallSection.getBodies());
            if (Strings.hasLength(catchParam)) {
                String catchStatusCode;
                if (catches.containsKey(catchParam)) {
                    catchStatusCode = catches.get(catchParam).v1();
                } else if (catchParam.startsWith("/") && catchParam.endsWith("/")) {
                    catchStatusCode = "4xx|5xx";
                } else {
                    throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
                }
                fail(formatStatusCodeMessage(restResponse, catchStatusCode));
            }
        } catch(RestException e) {
            if (!Strings.hasLength(catchParam)) {
                fail(formatStatusCodeMessage(e.restResponse(), "2xx"));
            } else if (catches.containsKey(catchParam)) {
                assertStatusCode(e.restResponse());
            } else if (catchParam.length() > 2 && catchParam.startsWith("/") && catchParam.endsWith("/")) {
                //the text of the error message matches regular expression
                assertThat(formatStatusCodeMessage(e.restResponse(), "4xx|5xx"), e.statusCode(), greaterThanOrEqualTo(400));
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

    private void assertStatusCode(RestResponse restResponse) {
        Tuple<String, org.hamcrest.Matcher<Integer>> stringMatcherTuple = catches.get(catchParam);
        assertThat(formatStatusCodeMessage(restResponse, stringMatcherTuple.v1()),
                restResponse.getStatusCode(), stringMatcherTuple.v2());
    }

    private String formatStatusCodeMessage(RestResponse restResponse, String expected) {
        return "expected [" + expected + "] status code but api [" + apiCallSection.getApi() + "] returned ["
                + restResponse.getStatusCode() + " " + restResponse.getReasonPhrase() + "] [" + restResponse.getBodyAsString() + "]";
    }

    private static Map<String, Tuple<String, org.hamcrest.Matcher<Integer>>> catches = new HashMap<>();

    static {
        catches.put("missing", tuple("404", equalTo(404)));
        catches.put("conflict", tuple("409", equalTo(409)));
        catches.put("forbidden", tuple("403", equalTo(403)));
        catches.put("request_timeout", tuple("408", equalTo(408)));
        catches.put("request", tuple("4xx|5xx", allOf(greaterThanOrEqualTo(400), not(equalTo(404)), not(equalTo(408)), not(equalTo(409)), not(equalTo(403)))));
    }
}
