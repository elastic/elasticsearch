/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ParsedMediaTypeTests extends ESTestCase {

    MediaTypeRegistry<XContentType> mediaTypeRegistry = new MediaTypeRegistry<XContentType>().register(XContentType.values());

    public void testCanonicalParsing() {
        assertThat(ParsedMediaType.parseMediaType("application/json").toMediaType(mediaTypeRegistry), equalTo(XContentType.JSON));
        assertThat(ParsedMediaType.parseMediaType("application/yaml").toMediaType(mediaTypeRegistry), equalTo(XContentType.YAML));
        assertThat(ParsedMediaType.parseMediaType("application/smile").toMediaType(mediaTypeRegistry), equalTo(XContentType.SMILE));
        assertThat(ParsedMediaType.parseMediaType("application/cbor").toMediaType(mediaTypeRegistry), equalTo(XContentType.CBOR));

        assertThat(
            ParsedMediaType.parseMediaType("application/vnd.elasticsearch+json;compatible-with=7").toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_JSON)
        );
        assertThat(
            ParsedMediaType.parseMediaType("application/vnd.elasticsearch+yaml;compatible-with=7").toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_YAML)
        );
        assertThat(
            ParsedMediaType.parseMediaType("application/vnd.elasticsearch+smile;compatible-with=7").toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_SMILE)
        );
        assertThat(
            ParsedMediaType.parseMediaType("application/vnd.elasticsearch+cbor;compatible-with=7").toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_CBOR)
        );
    }

    public void testJsonWithParameters() throws Exception {
        String mediaType = "application/vnd.elasticsearch+json";
        assertThat(ParsedMediaType.parseMediaType(mediaType).getParameters(), equalTo(Collections.emptyMap()));
        assertThat(ParsedMediaType.parseMediaType(mediaType + ";").getParameters(), equalTo(Collections.emptyMap()));
        assertThat(ParsedMediaType.parseMediaType(mediaType + "; charset=UTF-8").getParameters(), equalTo(Map.of("charset", "utf-8")));
        assertThat(
            ParsedMediaType.parseMediaType(mediaType + "; compatible-with=123;charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "compatible-with", "123"))
        );
    }

    public void testWhiteSpaceInTypeSubtype() {
        String mediaType = " application/vnd.elasticsearch+json ";
        assertThat(ParsedMediaType.parseMediaType(mediaType).toMediaType(mediaTypeRegistry), equalTo(XContentType.VND_JSON));

        assertThat(
            ParsedMediaType.parseMediaType(mediaType + "; compatible-with=123; charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "compatible-with", "123"))
        );
        assertThat(
            ParsedMediaType.parseMediaType(mediaType + "; compatible-with=123;\n charset=UTF-8").getParameters(),
            equalTo(Map.of("charset", "utf-8", "compatible-with", "123"))
        );
    }

    public void testInvalidParameters() {
        String mediaType = "application/vnd.elasticsearch+json";
        expectThrows(
            IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "; keyvalueNoEqualsSign").toMediaType(mediaTypeRegistry)
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "; key=").toMediaType(mediaTypeRegistry)
        );
    }

    public void testXContentTypes() {
        for (XContentType xContentType : XContentType.values()) {
            ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(xContentType.mediaTypeWithoutParameters());
            assertEquals(xContentType.mediaTypeWithoutParameters(), parsedMediaType.mediaTypeWithoutParameters());
        }
    }

    public void testWithParameters() {
        String mediaType = "application/foo";
        assertEquals(Collections.emptyMap(), ParsedMediaType.parseMediaType(mediaType).getParameters());
        assertEquals(Collections.emptyMap(), ParsedMediaType.parseMediaType(mediaType + ";").getParameters());
        assertEquals(Map.of("charset", "utf-8"), ParsedMediaType.parseMediaType(mediaType + "; charset=UTF-8").getParameters());
        assertEquals(
            Map.of("charset", "utf-8", "compatible-with", "123"),
            ParsedMediaType.parseMediaType(mediaType + "; compatible-with=123;charset=UTF-8").getParameters()
        );
    }

    public void testEmptyParams() {
        String mediaType = "application/foo";
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaType + randomFrom("", " ", ";", ";;", ";;;"));
        assertEquals("application/foo", parsedMediaType.mediaTypeWithoutParameters());
        assertEquals(Collections.emptyMap(), parsedMediaType.getParameters());
    }

    public void testMalformedParameters() {
        String mediaType = "application/foo";
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "; charsetunknown")
        );
        assertThat(exception.getMessage(), equalTo("invalid parameters for header [application/foo; charsetunknown]"));

        exception = expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + "; char=set=unknown"));
        assertThat(exception.getMessage(), equalTo("invalid parameters for header [application/foo; char=set=unknown]"));

        // do not allow white space in parameters between `=`
        exception = expectThrows(
            IllegalArgumentException.class,
            () -> ParsedMediaType.parseMediaType(mediaType + "    ;  compatible-with =  123  ;  charset=UTF-8")
        );
        assertThat(
            exception.getMessage(),
            equalTo("invalid parameters for header [application/foo    ;  compatible-with =  123  ;  charset=UTF-8]")
        );

        expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + ";k =y"));
        expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + ";k= y"));
        expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + ";k = y"));
        expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + ";= y"));
        expectThrows(IllegalArgumentException.class, () -> ParsedMediaType.parseMediaType(mediaType + ";k="));
    }

    public void testIgnoredMediaTypes() {
        // When using curl */* is used a default Accept header when not specified by a user
        assertThat(ParsedMediaType.parseMediaType("*/*"), is(nullValue()));

        // This media type is defined in sun.net.www.protocol.http.HttpURLConnection as a default Accept header
        // and used when a header was not set on a request
        // It should be treated as if a user did not specify a header value
        String mediaType = "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2";
        assertThat(ParsedMediaType.parseMediaType(mediaType), is(nullValue()));

        // example accept header used by a browser
        mediaType = "text/html,application/xhtml+xml,application/xml;q=0.9,"
            + "image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9";
        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(mediaType);
        assertThat(parsedMediaType, equalTo(null));
    }

    public void testParseMediaTypeFromXContentType() {

        assertThat(
            ParsedMediaType.parseMediaType(XContentType.YAML, Collections.emptyMap()).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.YAML)
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.SMILE, Collections.emptyMap()).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.SMILE)
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.CBOR, Collections.emptyMap()).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.CBOR)
        );

        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_JSON, Map.of("compatible-with", "7")).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_JSON)
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_YAML, Map.of("compatible-with", "7")).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_YAML)
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_SMILE, Map.of("compatible-with", "7")).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_SMILE)
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_CBOR, Map.of("compatible-with", "7")).toMediaType(mediaTypeRegistry),
            equalTo(XContentType.VND_CBOR)
        );
    }

    public void testResponseContentTypeHeader() {
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.JSON, Collections.emptyMap()).responseContentTypeHeader(),
            equalTo("application/json")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.YAML, Collections.emptyMap()).responseContentTypeHeader(),
            equalTo("application/yaml")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.SMILE, Collections.emptyMap()).responseContentTypeHeader(),
            equalTo("application/smile")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.CBOR, Collections.emptyMap()).responseContentTypeHeader(),
            equalTo("application/cbor")
        );

        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_JSON, Map.of("compatible-with", "7")).responseContentTypeHeader(),
            equalTo("application/vnd.elasticsearch+json;compatible-with=7")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_YAML, Map.of("compatible-with", "7")).responseContentTypeHeader(),
            equalTo("application/vnd.elasticsearch+yaml;compatible-with=7")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_SMILE, Map.of("compatible-with", "7")).responseContentTypeHeader(),
            equalTo("application/vnd.elasticsearch+smile;compatible-with=7")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.VND_CBOR, Map.of("compatible-with", "7")).responseContentTypeHeader(),
            equalTo("application/vnd.elasticsearch+cbor;compatible-with=7")
        );

        assertThat(
            ParsedMediaType.parseMediaType(XContentType.JSON, Map.of("charset", "utf-8")).responseContentTypeHeader(),
            equalTo("application/json;charset=utf-8")
        );
        assertThat(
            ParsedMediaType.parseMediaType(XContentType.JSON, Map.of("charset", "UTF-8")).responseContentTypeHeader(),
            equalTo("application/json;charset=UTF-8")
        );
    }
}
