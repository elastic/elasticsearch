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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class CompatibleHeaderCombinationTests extends ESTestCase {
    int CURRENT_VERSION = Version.CURRENT.major;
    int PREVIOUS_VERSION = Version.CURRENT.major - 1;
    int OBSOLETE_VERSION = Version.CURRENT.major - 2;

    public void testAcceptAndContentTypeCombinations() {
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent()));

        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));
        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent()));

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        //tests when body present and one of the headers missing - versioning is required on both when body is present
        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyPresent()));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyPresent()));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyPresent()));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyPresent()));

        //tests when body NOT present and one of the headers missing
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        //body not present - accept header is missing - it will default to Current version. Version on content type is ignored
        assertThat(requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader(null), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        //Accept header = application/json means current version. If body is provided then accept and content-Type should be the same
        assertThat(requestWith(acceptHeader("application/json"), contentTypeHeader(null), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader("application/json"), contentTypeHeader("application/json"), bodyPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader("application/json"), bodyPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));
    }

    public void testObsoleteVersion() {
        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(OBSOLETE_VERSION), bodyPresent()));

        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(null), bodyNotPresent()));
    }


    public void testMediaTypeCombinations() {
        //body not present - ignore content-type
        assertThat(requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader("application/json"), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader("*/*"), contentTypeHeader("application/json"), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        //this is for instance used by SQL
        assertThat(requestWith(acceptHeader("application/json"), contentTypeHeader("application/cbor"), bodyPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader("application/vnd.elasticsearch+json;compatible-with=7"),
            contentTypeHeader("application/vnd.elasticsearch+cbor;compatible-with=7"), bodyPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        //different versions on different media types
        expectThrows(RestRequest.CompatibleApiHeadersCombinationException.class, () ->
            requestWith(acceptHeader("application/vnd.elasticsearch+json;compatible-with=7"),
                contentTypeHeader("application/vnd.elasticsearch+cbor;compatible-with=8"), bodyPresent()));
    }

    public void testTextMediaTypes() {
        assertThat(requestWith(acceptHeader("text/tab-separated-values"), contentTypeHeader("application/json"), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader("text/plain"), contentTypeHeader("application/json"), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        assertThat(requestWith(acceptHeader("text/csv"), contentTypeHeader("application/json"), bodyNotPresent()),
            Matchers.allOf(requestCreated(), not(isCompatible())));

        //versioned
        assertThat(requestWith(acceptHeader("text/vnd.elasticsearch+tab-separated-values;compatible-with=7"),
            contentTypeHeader(7), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        assertThat(requestWith(acceptHeader("text/vnd.elasticsearch+plain;compatible-with=7"),
            contentTypeHeader(7), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));

        assertThat(requestWith(acceptHeader("text/vnd.elasticsearch+csv;compatible-with=7"),
            contentTypeHeader(7), bodyNotPresent()),
            Matchers.allOf(requestCreated(), isCompatible()));
    }

    private Matcher<RestRequest> requestCreated() {
        return Matchers.not(nullValue(RestRequest.class));
    }

    private Matcher<RestRequest> isCompatible() {
        return requestHasVersion(Version.CURRENT.major - 1);
    }

    private Matcher<RestRequest> requestHasVersion(int version) {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(build ->
                build.param(CompatibleConstants.COMPATIBLE_PARAMS_KEY) //TODO to be refactored into getVersion
            , equalTo(String.valueOf(version)));
    }

    private String bodyNotPresent() {
        return null;
    }

    private String bodyPresent() {
        return "some body";
    }

    private List<String> contentTypeHeader(int version) {
        return mediaType(version);
    }

    private List<String> acceptHeader(int version) {
        return mediaType(version);
    }

    private List<String> acceptHeader(String value) {
        return headerValue(value);
    }

    private List<String> contentTypeHeader(String value) {
        return headerValue(value);
    }

    private List<String> headerValue(String value) {
        if (value != null) {
            return List.of(value);
        }
        return null;
    }

    private List<String> mediaType(Integer version) {
        if (version != null) {
            return List.of("application/vnd.elasticsearch+json;compatible-with=" + version);
        }
        return null;
    }

    private RestRequest requestWith(List<String> accept, List<String> contentType, String body) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);

        builder.withHeaders(createHeaders(accept, contentType));
        if (body != null) {
            // xContentType header is set explicitly in headers
            builder.withContent(new BytesArray(body), null);
        }
        return builder.build();
    }

    private Map<String, List<String>> createHeaders(List<String> accept, List<String> contentType) {
        Map<String, List<String>> headers = new HashMap<>();
        if (accept != null) {
            headers.put("Accept", accept);
        }
        if (contentType != null) {
            headers.put("Content-Type", contentType);
        }
        return headers;
    }
}
