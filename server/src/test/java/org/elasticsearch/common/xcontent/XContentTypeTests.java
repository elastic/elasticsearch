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
package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

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
        assertThat(XContentType.fromMediaType(""), nullValue());
        assertThat(XContentType.fromMediaType("text/plain"), nullValue());
        assertThat(XContentType.fromMediaType("gobbly;goop"), nullValue());
    }

    public void testVersionedMediaType() {
        String version = String.valueOf(randomNonNegativeByte());
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+json;compatible-with=" + version),
            equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+cbor;compatible-with=" + version),
            equalTo(XContentType.CBOR));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+smile;compatible-with=" + version),
            equalTo(XContentType.SMILE));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+yaml;compatible-with=" + version),
            equalTo(XContentType.YAML));
        assertThat(XContentType.fromMediaType("application/json"),
            equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+x-ndjson;compatible-with=" + version),
            equalTo(XContentType.JSON));


        assertThat(XContentType.fromMediaType("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version),
            equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType("APPLICATION/JSON"),
            equalTo(XContentType.JSON));
    }

    public void testVersionParsing() {
        byte version = randomNonNegativeByte();
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+json;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+cbor;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+smile;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/vnd.elasticsearch+x-ndjson;compatible-with=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("application/json"),
            nullValue());


        assertThat(XContentType.parseVersion("APPLICATION/VND.ELASTICSEARCH+JSON;COMPATIBLE-WITH=" + version),
            equalTo(version));
        assertThat(XContentType.parseVersion("APPLICATION/JSON"),
            nullValue());

        assertThat(XContentType.parseVersion("application/json;compatible-with=" + version + ".0"),
            is(nullValue()));
    }

    public void testUnrecognizedParameter() {
        assertThat(XContentType.parseVersion("application/json; sth=123"),
            is(nullValue()));    }

    public void testMediaTypeWithoutESSubtype() {
        String version = String.valueOf(randomNonNegativeByte());
        assertThat(XContentType.fromMediaType("application/json;compatible-with=" + version), nullValue());
    }

    public void testAnchoring() {
        String version = String.valueOf(randomNonNegativeByte());
        assertThat(XContentType.fromMediaType("sth_application/json;compatible-with=" + version + ".0"), nullValue());
        assertThat(XContentType.fromMediaType("sth_application/json;compatible-with=" + version + "_sth"), nullValue());
        assertThat(XContentType.fromMediaType("application/json;compatible-with=" + version + "_sth"), nullValue());
    }
}
