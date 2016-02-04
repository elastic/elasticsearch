package org.elasticsearch.common.xcontent;

import org.elasticsearch.test.ESTestCase;

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
}
