/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SemanticOriginalValueEncoderTests extends ESTestCase {

    public void testTextRoundTrip() throws IOException {
        String text = randomAlphaOfLengthBetween(0, 50);
        XContentBuilder expected = JsonXContent.contentBuilder().startObject().field("f", text).endObject();
        assertThat(decode(SemanticOriginalValueEncoder.encode(text)), equalTo(Strings.toString(expected)));
    }

    public void testDataUriRoundTrip() throws IOException {
        // The base64 payload is decoded on the way in and regenerated on the way out, so the data URI must come back identical for
        // every non-text data type. The type is stored, so it is preserved independently of the media type. Empty payload is an edge.
        DataType dataType = randomFrom(DataType.IMAGE, DataType.AUDIO, DataType.VIDEO, DataType.PDF);
        String mediaType = randomFrom("image/png", "audio/mpeg", "video/mp4", "application/pdf");
        byte[] payload = randomBoolean() ? new byte[0] : randomByteArrayOfLength(randomIntBetween(1, 256));
        InferenceString value = new InferenceString(
            dataType,
            "data:" + mediaType + ";base64," + Base64.getEncoder().encodeToString(payload)
        );

        XContentBuilder expected = JsonXContent.contentBuilder().startObject().field("f");
        value.toXContent(expected, ToXContent.EMPTY_PARAMS);
        expected.endObject();

        assertThat(decode(SemanticOriginalValueEncoder.encode(value)), equalTo(Strings.toString(expected)));
    }

    public void testDecodeReturnsSourceValue() throws IOException {
        // decode() (used by the doc-values value fetcher) returns the value's _source form: a plain string for text...
        String text = randomAlphaOfLengthBetween(0, 50);
        assertThat(SemanticOriginalValueEncoder.decode(SemanticOriginalValueEncoder.encode(text)), equalTo(text));

        // ...and a {type, format, value} map for a data URI, matching the reconstructed _source object.
        DataType dataType = randomFrom(DataType.IMAGE, DataType.AUDIO, DataType.VIDEO, DataType.PDF);
        String mediaType = randomFrom("image/png", "audio/mpeg", "video/mp4", "application/pdf");
        byte[] payload = randomByteArrayOfLength(randomIntBetween(1, 64));
        String dataUri = "data:" + mediaType + ";base64," + Base64.getEncoder().encodeToString(payload);

        Object decoded = SemanticOriginalValueEncoder.decode(SemanticOriginalValueEncoder.encode(new InferenceString(dataType, dataUri)));
        assertThat(
            decoded,
            equalTo(
                Map.of(
                    InferenceString.TYPE_FIELD,
                    dataType.toString(),
                    InferenceString.FORMAT_FIELD,
                    dataType.getDefaultFormat().toString(),
                    InferenceString.VALUE_FIELD,
                    dataUri
                )
            )
        );
    }

    public void testEncodeInferenceStringText() {
        // A text value supplied as an InferenceString encodes the same as the raw string.
        String text = randomAlphaOfLengthBetween(1, 20);
        assertThat(SemanticOriginalValueEncoder.encode(InferenceString.ofText(text)), equalTo(SemanticOriginalValueEncoder.encode(text)));
    }

    public void testInvalidBase64IsRejected() {
        // Strict policy: a structurally valid data URI (so InferenceString accepts it) whose base64 payload is malformed is rejected
        // at encode (index) time rather than stored unparsed, with a clear message.
        InferenceString value = new InferenceString(DataType.IMAGE, "data:image/png;base64,@@@not-base64");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SemanticOriginalValueEncoder.encode(value));
        assertThat(e.getMessage(), containsString("Invalid base64 payload"));
    }

    private static String decode(BytesRef encoded) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject().field("f");
        SemanticOriginalValueEncoder.decodeAndWrite(encoded, builder);
        builder.endObject();
        return Strings.toString(builder);
    }
}
