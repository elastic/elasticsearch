/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchMatchers;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RestCompatibleVersionHelperTests extends ESTestCase {
    int CURRENT_VERSION = RestApiVersion.current().major;
    int PREVIOUS_VERSION = RestApiVersion.current().major - 1;
    int OBSOLETE_VERSION = RestApiVersion.current().major - 2;

    public void testAcceptAndContentTypeCombinations() {
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent()), isCompatible());

        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), isCompatible());

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version and the compatible versions must match. "
                    + "Accept="
                    + acceptHeader(PREVIOUS_VERSION)
                    + ", Content-Type="
                    + contentTypeHeader(CURRENT_VERSION)
            )
        );

        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()), isCompatible());
        // no body - content-type is ignored
        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyNotPresent()), not(isCompatible()));

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(PREVIOUS_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version and the compatible versions must match. "
                    + "Accept="
                    + acceptHeader(CURRENT_VERSION)
                    + ", Content-Type="
                    + contentTypeHeader(PREVIOUS_VERSION)
            )
        );

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyPresent()), not(isCompatible()));

        assertThat(requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(CURRENT_VERSION), bodyNotPresent()), not(isCompatible()));

        // tests when body present and one of the headers missing - versioning is required on both when body is present
        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(null), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version and the compatible versions must match. "
                    + "Accept="
                    + acceptHeader(PREVIOUS_VERSION)
                    + ", Content-Type="
                    + contentTypeHeader(null)
            )
        );

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(CURRENT_VERSION), contentTypeHeader(null), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version. "
                    + "Accept="
                    + acceptHeader(CURRENT_VERSION)
                    + ", Content-Type="
                    + contentTypeHeader(null)
            )
        );

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(null), contentTypeHeader(CURRENT_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version. "
                    + "Accept="
                    + acceptHeader(null)
                    + ", Content-Type="
                    + contentTypeHeader(CURRENT_VERSION)
            )
        );

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(null), contentTypeHeader(PREVIOUS_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "A compatible version is required on both Content-Type and Accept headers "
                    + "if either one has requested a compatible version and the compatible versions must match. "
                    + "Accept="
                    + acceptHeader(null)
                    + ", Content-Type="
                    + contentTypeHeader(PREVIOUS_VERSION)
            )
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
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(OBSOLETE_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Accept version must be either version "
                    + CURRENT_VERSION
                    + " or "
                    + PREVIOUS_VERSION
                    + ", but found "
                    + OBSOLETE_VERSION
                    + ". "
                    + "Accept="
                    + acceptHeader(OBSOLETE_VERSION)
            )
        );

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(OBSOLETE_VERSION), contentTypeHeader(null), bodyNotPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Accept version must be either version "
                    + CURRENT_VERSION
                    + " or "
                    + PREVIOUS_VERSION
                    + ", but found "
                    + OBSOLETE_VERSION
                    + ". "
                    + "Accept="
                    + acceptHeader(OBSOLETE_VERSION)
            )
        );

        e = expectThrows(
            ElasticsearchStatusException.class,
            () -> requestWith(acceptHeader(PREVIOUS_VERSION), contentTypeHeader(OBSOLETE_VERSION), bodyPresent())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "Content-Type version must be either version "
                    + CURRENT_VERSION
                    + " or "
                    + PREVIOUS_VERSION
                    + ", but found "
                    + OBSOLETE_VERSION
                    + ". "
                    + "Content-Type="
                    + contentTypeHeader(OBSOLETE_VERSION)
            )
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
        assertThat(
            requestWith(
                acceptHeader("text/vnd.elasticsearch+tab-separated-values;compatible-with=7"),
                contentTypeHeader(7),
                bodyNotPresent()
            ),
            isCompatible()
        );

        assertThat(
            requestWith(acceptHeader("text/vnd.elasticsearch+plain;compatible-with=7"), contentTypeHeader(7), bodyNotPresent()),
            isCompatible()
        );

        assertThat(
            requestWith(acceptHeader("text/vnd.elasticsearch+csv;compatible-with=7"), contentTypeHeader(7), bodyNotPresent()),
            isCompatible()
        );
    }

    public void testVersionParsing() {
        byte version = randomNonNegativeByte();
        assertThat(
            RestCompatibleVersionHelper.parseVersion(
                ParsedMediaType.parseMediaType("application/vnd.elasticsearch+json;compatible-with=" + version)
            ),
            equalTo(version)
        );
        assertThat(
            RestCompatibleVersionHelper.parseVersion(
                ParsedMediaType.parseMediaType("application/vnd.elasticsearch+cbor;compatible-with=" + version)
            ),
            equalTo(version)
        );
        assertThat(
            RestCompatibleVersionHelper.parseVersion(
                ParsedMediaType.parseMediaType("application/vnd.elasticsearch+smile;compatible-with=" + version)
            ),
            equalTo(version)
        );
        assertThat(
            RestCompatibleVersionHelper.parseVersion(
                ParsedMediaType.parseMediaType("application/vnd.elasticsearch+x-ndjson;compatible-with=" + version)
            ),
            equalTo(version)
        );
        assertThat(RestCompatibleVersionHelper.parseVersion(ParsedMediaType.parseMediaType("application/json")), nullValue());

        assertThat(
            RestCompatibleVersionHelper.parseVersion(
                ParsedMediaType.parseMediaType("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version)
            ),
            equalTo(version)
        );
        assertThat(RestCompatibleVersionHelper.parseVersion(ParsedMediaType.parseMediaType("APPLICATION/JSON")), nullValue());

        assertThat(RestCompatibleVersionHelper.parseVersion(ParsedMediaType.parseMediaType("application/json; sth=123")), is(nullValue()));

    }

    private Matcher<RestApiVersion> isCompatible() {
        return requestHasVersion(PREVIOUS_VERSION);
    }

    private Matcher<RestApiVersion> requestHasVersion(int version) {
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

    private RestApiVersion requestWith(String accept, String contentType, String body) {
        ParsedMediaType parsedAccept = ParsedMediaType.parseMediaType(accept);
        ParsedMediaType parsedContentType = ParsedMediaType.parseMediaType(contentType);
        return RestCompatibleVersionHelper.getCompatibleVersion(parsedAccept, parsedContentType, body.isEmpty() == false);
    }

}
