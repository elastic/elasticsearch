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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.rest.RestTestExecutionContext;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.*;
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

        try {
            executionContext.callApi(apiCallSection.getApi(), apiCallSection.getParams(), apiCallSection.getBody());

        } catch(RestException e) {
            if (!Strings.hasLength(catchParam)) {
                fail(formatStatusCodeMessage(e.restResponse(), "2xx"));
            }

            if ("param".equals(catchParam)) {
                //client should throw validation error before sending request
                //lets just return without doing anything as we don't have any client to test here
                logger.info("found [catch: param], no request sent");
            } else if ("missing".equals(catchParam)) {
                assertThat(formatStatusCodeMessage(e.restResponse(), "404"), e.statusCode(), equalTo(404));
            } else if ("conflict".equals(catchParam)) {
                assertThat(formatStatusCodeMessage(e.restResponse(), "409"), e.statusCode(), equalTo(409));
            }  else if ("forbidden".equals(catchParam)) {
                assertThat(formatStatusCodeMessage(e.restResponse(), "403"), e.statusCode(), equalTo(403));
            } else if ("request".equals(catchParam)) {
                //generic error response from ES
                assertThat(formatStatusCodeMessage(e.restResponse(), "4xx|5xx"), e.statusCode(), greaterThanOrEqualTo(400));
            } else if (catchParam.startsWith("/") && catchParam.endsWith("/")) {
                //the text of the error message matches regular expression
                assertThat(formatStatusCodeMessage(e.restResponse(), "4xx|5xx"), e.statusCode(), greaterThanOrEqualTo(400));
                Object error = executionContext.response("error");
                assertThat("error was expected in the response", error, notNullValue());
                //remove delimiters from regex
                String regex = catchParam.substring(1, catchParam.length() - 1);
                String errorMessage = error.toString();
                Matcher matcher = Pattern.compile(regex).matcher(errorMessage);
                assertThat("error message [" + errorMessage + "] was expected to match [" + catchParam + "] but didn't",
                        matcher.find(), equalTo(true));
            } else {
                throw new UnsupportedOperationException("catch value [" + catchParam + "] not supported");
            }
        }
    }

    private String formatStatusCodeMessage(RestResponse restResponse, String expected) {
        return "expected [" + expected + "] status code but api [" + apiCallSection.getApi() + "] returned ["
                + restResponse.getStatusCode() + " " + restResponse.getReasonPhrase() + "] [" + restResponse.getBody() + "]";
    }
}
