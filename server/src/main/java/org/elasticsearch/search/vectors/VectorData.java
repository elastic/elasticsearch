/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;

public record VectorData(float[] floatVector, byte[] byteVector, String base64Vector) implements Writeable, ToXContentFragment {

    private static final TransportVersion QUERY_VECTOR_BASE64 = TransportVersion.fromName("knn_query_vector_base64");

    public VectorData(float[] floatVector) {
        this(floatVector, null, null);
    }

    public VectorData(byte[] byteVector) {
        this(null, byteVector, null);
    }

    public VectorData(String base64Vector) {
        this(null, null, base64Vector);
    }

    public VectorData(StreamInput in) throws IOException {
        this(in.readOptionalFloatArray(), in.readOptionalByteArray(), readOptionalBase64Vector(in));
    }

    public VectorData {
        int count = 0;
        if (floatVector != null) {
            count++;
        }
        if (byteVector != null) {
            count++;
        }
        if (base64Vector != null) {
            count++;
        }
        if (count != 1) {
            throw new IllegalArgumentException("please supply exactly one of a float vector, byte vector, or base64 vector");
        }
    }

    public boolean isFloat() {
        return floatVector != null;
    }

    public boolean isBase64() {
        return base64Vector != null;
    }

    public String base64Vector() {
        return base64Vector;
    }

    public byte[] asByteVector() {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (byteVector != null) {
            return byteVector;
        }
        DenseVectorFieldMapper.BYTE_ELEMENT.checkVectorBounds(floatVector);
        byte[] vec = new byte[floatVector.length];
        for (int i = 0; i < floatVector.length; i++) {
            vec[i] = (byte) floatVector[i];
        }
        return vec;
    }

    public float[] asFloatVector() {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (floatVector != null) {
            return floatVector;
        }
        float[] vec = new float[byteVector.length];
        for (int i = 0; i < byteVector.length; i++) {
            vec[i] = byteVector[i];
        }
        return vec;
    }

    public void addToBuffer(DenseVectorFieldMapper.Element element, ByteBuffer byteBuffer) {
        if (base64Vector != null) {
            throw new IllegalStateException("base64 query vector must be resolved against the field type before use");
        }
        if (floatVector != null) {
            element.writeValues(byteBuffer, floatVector);
        } else {
            byteBuffer.put(byteVector);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (base64Vector != null && out.getTransportVersion().supports(QUERY_VECTOR_BASE64) == false) {
            throw new IllegalArgumentException(
                "query_vector contains base64 but transport version ["
                    + out.getTransportVersion()
                    + "] does not support base64 query vectors"
            );
        }
        out.writeOptionalFloatArray(floatVector);
        out.writeOptionalByteArray(byteVector);
        if (out.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            out.writeOptionalString(base64Vector);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (floatVector != null) {
            builder.startArray();
            for (float v : floatVector) {
                builder.value(v);
            }
            builder.endArray();
        } else if (base64Vector != null) {
            builder.value(base64Vector);
        } else {
            builder.value(HexFormat.of().formatHex(byteVector));
        }
        return builder;
    }

    @Override
    public String toString() {
        if (floatVector != null) {
            return Arrays.toString(floatVector);
        }
        if (byteVector != null) {
            return Arrays.toString(byteVector);
        }
        return base64Vector;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VectorData other = (VectorData) obj;
        return Arrays.equals(floatVector, other.floatVector)
            && Arrays.equals(byteVector, other.byteVector)
            && Objects.equals(base64Vector, other.base64Vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(floatVector), Arrays.hashCode(byteVector), base64Vector);
    }

    public static VectorData parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        return switch (token) {
            case START_ARRAY -> parseQueryVectorArray(parser);
            case VALUE_STRING -> parseStringVector(parser);
            case VALUE_NUMBER -> parseNumberVector(parser);
            default -> throw new ParsingException(parser.getTokenLocation(), format("Unknown type [%s] for parsing vector", token));
        };
    }

    private static VectorData parseQueryVectorArray(XContentParser parser) throws IOException {
        XContentParser.Token token;
        List<Float> vectorArr = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_NUMBER || token == XContentParser.Token.VALUE_STRING) {
                vectorArr.add(parser.floatValue());
            } else {
                throw new ParsingException(parser.getTokenLocation(), format("Type [%s] not supported for query vector", token));
            }
        }
        float[] floatVector = new float[vectorArr.size()];
        for (int i = 0; i < vectorArr.size(); i++) {
            floatVector[i] = vectorArr.get(i);
        }
        return VectorData.fromFloats(floatVector);
    }

    private static VectorData parseStringVector(XContentParser parser) throws IOException {
        String text = parser.text();
        if (looksLikeHex(text)) {
            if ((text.length() & 1) == 1) {
                throw new ParsingException(parser.getTokenLocation(), "[query_vector] must be a valid hex string");
            }
            return VectorData.fromBytes(HexFormat.of().parseHex(text));
        }
        return VectorData.fromBase64(text);
    }

    private static VectorData parseNumberVector(XContentParser parser) throws IOException {
        return VectorData.fromFloats(new float[] { parser.floatValue() });
    }

    public static VectorData fromFloats(float[] vec) {
        return vec == null ? null : new VectorData(vec);
    }

    public static VectorData fromBytes(byte[] vec) {
        return vec == null ? null : new VectorData(vec);
    }

    public static VectorData fromBase64(String base64) {
        return base64 == null ? null : new VectorData(null, null, base64);
    }

    public VectorData resolveBase64(DenseVectorFieldType vectorFieldType) {
        if (base64Vector == null) {
            return this;
        }
        return decodeBase64Vector(base64Vector, vectorFieldType);
    }

    private static String readOptionalBase64Vector(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(QUERY_VECTOR_BASE64)) {
            return in.readOptionalString();
        }
        return null;
    }

    private static boolean looksLikeHex(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            boolean isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (isHex == false) {
                return false;
            }
        }
        return text.isEmpty() == false;
    }

    private static byte[] decodeBase64Bytes(String base64String) {
        try {
            return Base64.getDecoder().decode(base64String);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[query_vector] must be a valid base64 string: " + e.getMessage(), e);
        }
    }

    private static VectorData decodeBase64Vector(String base64String, DenseVectorFieldType vectorFieldType) {
        byte[] bytes = decodeBase64Bytes(base64String);

        final DenseVectorFieldMapper.ElementType elementType;
        if (vectorFieldType != null) {
            elementType = vectorFieldType.getElementType();
        } else {
            elementType = bytes.length % Float.BYTES == 0
                ? DenseVectorFieldMapper.ElementType.FLOAT
                : DenseVectorFieldMapper.ElementType.BYTE;
        }

        final VectorData decoded = switch (elementType) {
            case FLOAT, BFLOAT16 -> {
                if (bytes.length % Float.BYTES != 0) {
                    throw new IllegalArgumentException(
                        "["
                            + "query_vector"
                            + "] must contain a valid Base64-encoded float vector, "
                            + "but the decoded bytes length ["
                            + bytes.length
                            + "] is not a multiple of "
                            + Float.BYTES
                    );
                }
                int numFloats = bytes.length / Float.BYTES;
                float[] floats = new float[numFloats];
                ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
                for (int i = 0; i < numFloats; i++) {
                    floats[i] = buffer.getFloat();
                }
                yield VectorData.fromFloats(floats);
            }
            case BYTE, BIT -> VectorData.fromBytes(bytes);
        };

        if (vectorFieldType != null) {
            DenseVectorFieldMapper.Element element = DenseVectorFieldMapper.Element.getElement(elementType);
            int dims = decoded.isFloat() ? decoded.asFloatVector().length : decoded.asByteVector().length;
            element.checkDimensions(vectorFieldType.getVectorDimensions(), dims);
            if (decoded.isFloat()) {
                element.checkVectorBounds(decoded.asFloatVector());
            }
        }
        return decoded;
    }

}
