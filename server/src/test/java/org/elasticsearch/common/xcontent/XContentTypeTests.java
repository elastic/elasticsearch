/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class XContentTypeTests extends ESTestCase {

    public void testFromJson() throws Exception {
        String mediaType = "application/json";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromJsonUppercase() throws Exception {
        String mediaType = "application/json".toUpperCase(Locale.ROOT);
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromYaml() throws Exception {
        String mediaType = "application/yaml";
        XContentType expectedXContentType = XContentType.YAML;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + "; charset=UTF-8"), equalTo(expectedXContentType));
    }

    public void testFromSmile() throws Exception {
        String mediaType = "application/smile";
        XContentType expectedXContentType = XContentType.SMILE;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromCbor() throws Exception {
        String mediaType = "application/cbor";
        XContentType expectedXContentType = XContentType.CBOR;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcard() throws Exception {
        String mediaType = "application/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromWildcardUppercase() throws Exception {
        String mediaType = "APPLICATION/*";
        XContentType expectedXContentType = XContentType.JSON;
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType), equalTo(expectedXContentType));
        assertThat(XContentType.fromMediaTypeOrFormat(mediaType + ";"), equalTo(expectedXContentType));
    }

    public void testFromRubbish() throws Exception {
        assertThat(XContentType.fromMediaTypeOrFormat(null), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat(""), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("text/plain"), nullValue());
        assertThat(XContentType.fromMediaTypeOrFormat("gobbly;goop"), nullValue());
    }

    public void testVersionedMediaType() throws Exception {
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+json;compatible-with=7"), equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+yaml;compatible-with=7"), equalTo(XContentType.YAML));
        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+cbor;compatible-with=7"), equalTo(XContentType.CBOR));
        assertThat(
            XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+smile;compatible-with=7"),
            equalTo(XContentType.SMILE)
        );

        assertThat(XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+json ;compatible-with=7"), equalTo(XContentType.JSON));
        assertThat(
            XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+json ;compatible-with=7 ; charset=utf-8"),
            equalTo(XContentType.JSON)
        );
        assertThat(
            XContentType.fromMediaTypeOrFormat("application/vnd.elasticsearch+json;charset=utf-8;compatible-with=7"),
            equalTo(XContentType.JSON)
        );

        // we don't expect charset parameters when using fromMediaType
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+json;compatible-with=7"), equalTo(XContentType.JSON));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+yaml;compatible-with=7"), equalTo(XContentType.YAML));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+cbor;compatible-with=7"), equalTo(XContentType.CBOR));
        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+smile;compatible-with=7"), equalTo(XContentType.SMILE));

        assertThat(XContentType.fromMediaType("application/vnd.elasticsearch+json ;compatible-with=7"), equalTo(XContentType.JSON));

    }
}
