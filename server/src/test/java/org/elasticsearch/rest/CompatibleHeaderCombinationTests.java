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
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class CompatibleHeaderCombinationTests extends ESTestCase {
    int CURRENT_VERSION = Version.CURRENT.major;
    int PREVIOUS_VERSION = Version.CURRENT.major - 1;

    public void testAcceptAndContentTypeCombinations() {
        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent(),
            expect(requestCreated(), isCompatible()));

        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent(),
            expect(requestCreated(), isCompatible()));

        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent(),
            expect(requestCreated(), isCompatible())); // no body - content-type is ignored

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent(),
            expect(requestCreated(), not(isCompatible()))); // no body - content-type is ignored

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent(),
            expect(requestCreated(), not(isCompatible())));

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent(),
            expect(requestCreated(), not(isCompatible())));

        //tests when body present and one of the headers missing
        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        createRequestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        createRequestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyPresent(),
            expect(exceptionDuringCreation(RestRequest.CompatibleApiHeadersCombinationException.class)));

        //tests when body NOT present and one of the headers missing
        createRequestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyNotPresent(),
            expect(requestCreated(), isCompatible()));

        createRequestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyNotPresent(),
            expect(requestCreated(), not(isCompatible())));

// test cases from the doc, but not sure why would these be ok when accept is null
//        createRequestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent(),
//            expect(requestCreated(), isCompatible()));
//        createRequestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyNotPresent(),
//            expect(requestCreated(), isCompatible()));
    }


    private Matcher<FakeRestRequest.Builder> exceptionDuringCreation(Class<? extends Exception> exceptionClass) {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> {
            try {
                builder.build();
            } catch (Exception e) {
                e.printStackTrace();
                return e;
            }
            return null;
        }, instanceOf(exceptionClass));

    }

    private Matcher<FakeRestRequest.Builder> requestCreated() {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> builder.build(), Matchers.notNullValue());
    }

    private Matcher<FakeRestRequest.Builder> isCompatible() {
        return requestHasVersion(Version.CURRENT.major - 1);
    }

    private Matcher<FakeRestRequest.Builder> requestHasVersion(int version) {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(builder -> {
            FakeRestRequest build = builder.build();
            return build.param(CompatibleConstants.COMPATIBLE_PARAMS_KEY); //TODO to be refactored into getVersion
        }, equalTo(String.valueOf(version)));
    }

    private String bodyNotPresent() {
        return null;
    }

    private String bodyPresent() {
        return "some body";
    }

    private List<String> contentTypeHeader(Integer version) {
        return mediaType(version);
    }

    private List<String> acceptHeader(Integer version) {
        return mediaType(version);
    }

    private List<String> mediaType(Integer version) {
        if (version != null) {
            return List.of("application/vnd.elasticsearch+json;compatible-with=" + version);
        }
        return null;
    }

    private Matcher<FakeRestRequest.Builder> expect(Matcher<FakeRestRequest.Builder>... matchers) {
        return Matchers.allOf(matchers);
    }

    private void createRequestWith(List<String> accept,
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
