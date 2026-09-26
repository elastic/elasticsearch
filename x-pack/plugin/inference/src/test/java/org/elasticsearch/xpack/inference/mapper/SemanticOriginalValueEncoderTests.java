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
        InferenceString value = randomDataUriInferenceString();

        XContentBuilder expected = JsonXContent.contentBuilder().startObject().field("f");
        value.toXContent(expected, ToXContent.EMPTY_PARAMS);
        expected.endObject();

        assertThat(decode(SemanticOriginalValueEncoder.encode(value)), equalTo(Strings.toString(expected)));
    }

    public void testDecodeReturnsSourceValue() throws IOException {
        // decode() (used by the doc-values value fetcher) returns the value's _source form: a plain string for text...
        String text = randomAlphaOfLengthBetween(0, 50);
        assertThat(SemanticOriginalValueEncoder.decode(SemanticOriginalValueEncoder.encode(text)), equalTo(text));

        // ...and a {type, format, value} map for a data URI (empty payload included), matching the reconstructed _source object.
        InferenceString value = randomDataUriInferenceString();
        Object decoded = SemanticOriginalValueEncoder.decode(SemanticOriginalValueEncoder.encode(value));
        assertThat(
            decoded,
            equalTo(
                Map.of(
                    InferenceString.TYPE_FIELD,
                    value.dataType().toString(),
                    InferenceString.FORMAT_FIELD,
                    value.dataType().getDefaultFormat().toString(),
                    InferenceString.VALUE_FIELD,
                    value.value()
                )
            )
        );
    }

    public void testEncodeTextInferenceStringIsRejected() {
        // A text value must be supplied as a plain string; a text InferenceString is rejected here, mirroring
        // ShardBulkInferenceActionFilter, which does not allow InferenceStrings to represent text values.
        InferenceString text = InferenceString.ofText(randomAlphaOfLengthBetween(1, 20));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SemanticOriginalValueEncoder.encode(text));
        assertThat(e.getMessage(), containsString("Text values must be supplied as a string"));
    }

    /** A random non-text data URI {@link InferenceString}, with an empty payload as one possibility. */
    private static InferenceString randomDataUriInferenceString() {
        DataType dataType = randomFrom(DataType.IMAGE, DataType.AUDIO, DataType.VIDEO, DataType.PDF);
        String mediaType = randomFrom("image/png", "audio/mpeg", "video/mp4", "application/pdf");
        byte[] payload = randomBoolean() ? new byte[0] : randomByteArrayOfLength(randomIntBetween(1, 256));
        return new InferenceString(dataType, "data:" + mediaType + ";base64," + Base64.getEncoder().encodeToString(payload));
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
