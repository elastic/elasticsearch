/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchMatchers;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.*;

public class CompatibleHeaderCombinationTests extends ESTestCase {
    public void testNotACompatibleApiRequest() {
        // no body, and no Accept header - won't set compatible param. Not a compatible Request
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        FakeRestRequest restRequest = builder.build();
        assertThat(restRequest.param(CompatibleConstants.COMPATIBLE_PARAMS_KEY), nullValue());
    }

    public void testIncorrectCompatibleAcceptHeader() {
        testRestRequestCreation(correctV7Accept(), correctV7ContentType(), bodyPresent(),
            expect(requestCreated(), isCompatible()));

        testRestRequestCreation(correctV7Accept(), correctV7ContentType(), bodyNotPresent(),
            expect(requestCreated(), isCompatible()));

        testRestRequestCreation(correctV7Accept(), v8ContentType(), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));
    }

    private Matcher<FakeRestRequest.Builder> exceptionDuringCreation(Class<? extends Exception> exceptionClass) {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> {
            try {
                builder.build();
            } catch (Exception e) {
                return e;
            }
            return null;
        }, instanceOf(exceptionClass));

    }

    private Matcher<FakeRestRequest.Builder> requestCreated() {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> builder.build(), Matchers.notNullValue());
    }

    private Matcher<FakeRestRequest.Builder> isCompatible() {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> {
            FakeRestRequest build = builder.build();
            return build.param(CompatibleConstants.COMPATIBLE_PARAMS_KEY); //TODO to be refactored into getVersion
        }, equalTo(String.valueOf(Version.CURRENT.major - 1)));
    }

    private String bodyNotPresent() {
        return null;
    }

    private String bodyPresent() {
        return "some body";
    }

    private List<String> correctV7ContentType() {
        return mediaType(7);
    }

    private List<String> correctV7Accept() {
        return mediaType(7);
    }

    private List<String> v8ContentType() {
        return mediaType(8);
    }

    private List<String> mediaType(int version) {
        return List.of("application/vnd.elasticsearch+json;compatible-with=" + version);
    }

    private Matcher<FakeRestRequest.Builder> expect(Matcher<FakeRestRequest.Builder>... matchers) {
        return Matchers.allOf(matchers);
    }

    private void testRestRequestCreation(List<String> accept,
                                         List<String> contentType,
                                         String body,
                                         Matcher<FakeRestRequest.Builder> matcher) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);

        builder.withHeaders(createHeaders(accept, contentType));
        if (body != null) {
            // xContentType header is set explicitly in headers
            builder.withContent(new BytesArray(body), null);
        }
        assertThat(builder, matcher);
    }

    private Map<String, List<String>> createHeaders(List<String> accept, List<String> contentType) {
        Map<String, List<String>> headers = new HashMap<>();
        if (accept != null) {
            headers.put("Accept", accept);
        }
        if (accept != null) {
            headers.put("Content-Type", contentType);
        }
        return headers;
    }
}
