/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ParsedMediaType;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class XContentTypeTests extends ESTestCase {

    public void testFromJson() throws Exception {
        String mediaType = "application/json";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=utf-8"), equalTo(expectedXContentType));
    }

    public void testFromNdJson() throws Exception {
        String mediaType = "application/x-ndjson";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromJsonUppercase() throws Exception {
        String mediaType = "application/json".toUpperCase(Locale.ROOT);
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromYaml() throws Exception {
        String mediaType = "application/yaml";
        XContentType expectedXContentType = XContentType.YAML;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromSmile() throws Exception {
        String mediaType = "application/smile";
        XContentType expectedXContentType = XContentType.SMILE;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromCbor() throws Exception {
        String mediaType = "application/cbor";
        XContentType expectedXContentType = XContentType.CBOR;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcard() throws Exception {
        String mediaType = "application/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcardUppercase() throws Exception {
        String mediaType = "APPLICATION/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaType(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaType(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromRubbish() throws Exception {
        assertThat(XContentType.fromMediaType(null), nullValue());
        expectThrows(IllegalArgumentException.class, () -> XContentType.fromMediaType(""));
        expectThrows(IllegalArgumentException.class, () -> XContentType.fromMediaType("gobbly;goop"));
        assertThat(XContentType.fromMediaType("text/plain"), nullValue());
    }

    public void testVersionedMediaType() {
        String version = String.valueOf(randomNonNegativeByte());
        assertThat(
            XContentType.fromMediaType("application/vnd.elasticsearch+json;compatible-with=" + version),
            equalTo(XContentType.VND_JSON)
        );
        assertThat(
            XContentType.fromMediaType("application/vnd.elasticsearch+cbor;compatible-with=" + version),
            equalTo(XContentType.VND_CBOR)
        );
        assertThat(
            XContentType.fromMediaType("application/vnd.elasticsearch+smile;compatible-with=" + version),
            equalTo(XContentType.VND_SMILE)
        );
        assertThat(
            XContentType.fromMediaType("application/vnd.elasticsearch+yaml;compatible-with=" + version),
            equalTo(XContentType.VND_YAML)
        );
        assertThat(XContentType.fromMediaType("application/json"), equalTo(XContentType.JSON));
        assertThat(
            XContentType.fromMediaType("application/vnd.elasticsearch+x-ndjson;compatible-with=" + version),
            equalTo(XContentType.VND_JSON)
        );

        assertThat(
            XContentType.fromMediaType("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version),
            equalTo(XContentType.VND_JSON)
        );
        assertThat(XContentType.fromMediaType("APPLICATION/JSON"), equalTo(XContentType.JSON));
    }

    public void testVersionParsing() {
        byte version = randomNonNegativeByte();
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+json;compatible-with=" + version), equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+cbor;compatible-with=" + version), equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+smile;compatible-with=" + version), equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+x-ndjson;compatible-with=" + version), equalTo(version));
        assertThat(XContentType.parseVersion("application/json"), nullValue());

        assertThat(XContentType.parseVersion("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version), equalTo(version));
        assertThat(XContentType.parseVersion("APPLICATION/JSON"), nullValue());

        // validation is done when parsing a MediaType
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+json;compatible-with=" + version + ".0"), is(nullValue()));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+json;compatible-with=" + version + "_sth"), nullValue());
    }

    public void testUnrecognizedParameters() {
        // unrecognised parameters are ignored
        String version = String.valueOf(randomNonNegativeByte());

        assertThat(XContentType.fromMediaType("application/json;compatible-with=" + version), is(XContentType.JSON));
        // TODO do not allow parsing unrecognized parameter value https://github.com/elastic/elasticsearch/issues/63080
        // assertThat(XContentType.parseVersion("application/json;compatible-with=123"),
        // is(nullValue()));
    }

    public void testParsedMediaTypeImmutability() {
        ParsedMediaType xContentTypeJson = XContentType.JSON.toParsedMediaType();
        assertThat(xContentTypeJson.getParameters(), is(anEmptyMap()));

        ParsedMediaType parsedMediaType = ParsedMediaType.parseMediaType(XContentType.JSON, Map.of("charset", "utf-8"));
        assertThat(xContentTypeJson.getParameters(), is(anEmptyMap()));
        assertThat(parsedMediaType.getParameters(), equalTo(Map.of("charset", "utf-8")));

        Map<String, String> parameters = new HashMap<>(Map.of("charset", "utf-8"));
        parsedMediaType = ParsedMediaType.parseMediaType(XContentType.JSON, parameters);
        parameters.clear();
        assertThat(parsedMediaType.getParameters(), equalTo(Map.of("charset", "utf-8")));
    }
}
