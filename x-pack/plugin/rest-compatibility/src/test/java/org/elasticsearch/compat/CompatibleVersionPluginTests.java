/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchMatchers;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CompatibleVersionPluginTests extends ESTestCase {
    CompatibleVersionPlugin compatibleVersionPlugin = new CompatibleVersionPlugin();
    int CURRENT_VERSION = Version.CURRENT.major;
    int PREVIOUS_VERSION = Version.CURRENT.major - 1;
    int OBSOLETE_VERSION = Version.CURRENT.major - 2;

    public void testAcceptAndContentTypeCombinations() {
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent()), isCompatible());

        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), isCompatible());

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent())
        );

        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()), isCompatible());
        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), not(isCompatible()));

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent())
        );

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()), not(isCompatible()));

        // tests when body present and one of the headers missing - versioning is required on both when body is present
        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyPresent())
        );

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyPresent())
        );

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyPresent())
        );

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyPresent())
        );

        // tests when body NOT present and one of the headers missing
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyNotPresent()), isCompatible());

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyNotPresent()), not(isCompatible()));

        // body not present - accept header is missing - it will default to Current version. Version on content type is ignored
        assertThat(requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader(null), bodyNotPresent()), not(isCompatible()));

        // Accept header = application/json means current version. If body is provided then accept and content-Type should be the same
        assertThat(requestWith(acceptHeader("application/json"), contentTypeHeader(null), bodyNotPresent()), not(isCompatible()));

        assertThat(
            requestWith(acceptHeader("application/json"), contentTypeHeader("application/json"), bodyPresent()),
            not(isCompatible())
        );

        assertThat(requestWith(acceptHeader(null), contentTypeHeader("application/json"), bodyPresent()), not(isCompatible()));
    }

    public void testObsoleteVersion() {
        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(OBSOLETE_VERSION), bodyPresent())
        );

        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(null), bodyNotPresent())
        );
    }

    public void testMediaTypeCombinations() {
        // body not present - ignore content-type
        assertThat(requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader(null), contentTypeHeader("application/json"), bodyNotPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader("*/*"), contentTypeHeader("application/json"), bodyNotPresent()), not(isCompatible()));

        // this is for instance used by SQL
        assertThat(
            requestWith(acceptHeader("application/json"), contentTypeHeader("application/cbor"), bodyPresent()),
            not(isCompatible())
        );

        assertThat(
            requestWith(
                acceptHeader("application/vnd.elasticsearch+json;compatible-with=7"),
                contentTypeHeader("application/vnd.elasticsearch+cbor;compatible-with=7"),
                bodyPresent()
            ),
            isCompatible()
        );

        // different versions on different media types
        expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(
                acceptHeader("application/vnd.elasticsearch+json;compatible-with=7"),
                contentTypeHeader("application/vnd.elasticsearch+cbor;compatible-with=8"),
                bodyPresent()
            )
        );
    }

    public void testTextMediaTypes() {
        assertThat(
            requestWith(acceptHeader("text/tab-separated-values"), contentTypeHeader("application/json"), bodyNotPresent()),
            not(isCompatible())
        );

        assertThat(requestWith(acceptHeader("text/plain"), contentTypeHeader("application/json"), bodyNotPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader("text/csv"), contentTypeHeader("application/json"), bodyNotPresent()), not(isCompatible()));

        // versioned
        // assertThat(
        // requestWith(
        // acceptHeader("text/vnd.elasticsearch+tab-separated-values;compatible-with=7"),
        // contentTypeHeader(7),
        // bodyNotPresent()
        // ),
        // isCompatible()
        // );
        //
        // assertThat(
        // requestWith(acceptHeader("text/vnd.elasticsearch+plain;compatible-with=7"), contentTypeHeader(7), bodyNotPresent()),
        // isCompatible()
        // );
        //
        // assertThat(
        // requestWith(acceptHeader("text/vnd.elasticsearch+csv;compatible-with=7"), contentTypeHeader(7), bodyNotPresent()),
        // isCompatible()
        // );
    }

    private Matcher<Version> isCompatible() {
        return requestHasVersion(PREVIOUS_VERSION);
    }

    private Matcher<Version> requestHasVersion(int version) {
        return ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(v -> (int) v.major, equalTo(version));
    }

    private String bodyNotPresent() {
        return "";
    }

    private String bodyPresent() {
        return "some body";
    }

    private String contentTypeHeader(int version) {
        return mediaType(String.valueOf(version));
    }

    private String acceptHeader(int version) {
        return mediaType(String.valueOf(version));
    }

    private String acceptHeader(String value) {
        return value;
    }

    private String contentTypeHeader(String value) {
        return value;
    }

    private String mediaType(String version) {
        if (version != null) {
            return "application/vnd.elasticsearch+json;compatible-with=" + version;
        }
        return null;
    }

    private Version requestWith(String accept, String contentType, String body) {
        return compatibleVersionPlugin.getCompatibleVersion(accept, contentType, body.isEmpty() == false);
    }

}
