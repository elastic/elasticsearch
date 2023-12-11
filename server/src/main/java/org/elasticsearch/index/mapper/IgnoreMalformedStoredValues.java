/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

/**
 * Saves malformed values to stored fields so they can be loaded for synthetic
 * {@code _source}.
 */
public abstract class IgnoreMalformedStoredValues {
    /**
     * Build a {@link StoredField} for the value on which the parser is
     * currently positioned.
     * <p>
     * We try to use {@link StoredField}'s native types for fields where
     * possible but we have to preserve more type information than
     * stored fields support, so we encode all of those into stored fields'
     * {@code byte[]} type and then encode type information in the first byte.
     * </p>
     */
    public static StoredField storedField(String fieldName, XContentParser parser) throws IOException {
        String name = name(fieldName);
        return switch (parser.currentToken()) {
            case VALUE_STRING -> new StoredField(name, parser.text());
            case VALUE_NUMBER -> switch (parser.numberType()) {
                case INT -> new StoredField(name, parser.intValue());
                case LONG -> new StoredField(name, parser.longValue());
                case DOUBLE -> new StoredField(name, parser.doubleValue());
                case FLOAT -> new StoredField(name, parser.floatValue());
                case BIG_INTEGER -> new StoredField(name, encode((BigInteger) parser.numberValue()));
                case BIG_DECIMAL -> new StoredField(name, encode((BigDecimal) parser.numberValue()));
            };
            case VALUE_BOOLEAN -> new StoredField(name, new byte[] { parser.booleanValue() ? (byte) 't' : (byte) 'f' });
            case VALUE_EMBEDDED_OBJECT -> new StoredField(name, encode(parser.binaryValue()));
            case START_OBJECT, START_ARRAY -> {
                try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                    builder.copyCurrentStructure(parser);
                    yield new StoredField(name, encode(builder));
                }
            }
            default -> throw new IllegalArgumentException("synthetic _source doesn't support malformed objects");
        };
    }

    /**
     * Build a {@link IgnoreMalformedStoredValues} that never contains any values.
     */
    public static IgnoreMalformedStoredValues empty() {
        return EMPTY;
    }

    /**
     * Build a {@link IgnoreMalformedStoredValues} that loads from stored fields.
     */
    public static IgnoreMalformedStoredValues stored(String fieldName) {
        return new Stored(fieldName);
    }

    /**
     * A {@link Stream} mapping stored field paths to a place to put them
     * so they can be included in the next document.
     */
    public abstract Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders();

    /**
     * How many values has this field loaded for this document?
     */
    public abstract int count();

    /**
     * Write values for this document.
     */
    public abstract void write(XContentBuilder b) throws IOException;

    private static final Empty EMPTY = new Empty();

    private static class Empty extends IgnoreMalformedStoredValues {
        @Override
        public Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders() {
            return Stream.empty();
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {}
    }

    private static class Stored extends IgnoreMalformedStoredValues {
        private final String fieldName;

        private List<Object> values = emptyList();

        Stored(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Stream<Map.Entry<String, SourceLoader.SyntheticFieldLoader.StoredFieldLoader>> storedFieldLoaders() {
            return Stream.of(Map.entry(name(fieldName), values -> this.values = values));
        }

        @Override
        public int count() {
            return values.size();
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            for (Object v : values) {
                if (v instanceof BytesRef r) {
                    decodeAndWrite(b, r);
                } else {
                    b.value(v);
                }
            }
            values = emptyList();
        }

        private static void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
            switch (r.bytes[r.offset]) {
                case 'b':
                    b.value(r.bytes, r.offset + 1, r.length - 1);
                    return;
                case 'c':
                    decodeAndWriteXContent(b, XContentType.CBOR, r);
                    return;
                case 'd':
                    if (r.length < 5) {
                        throw new IllegalArgumentException("Can't decode " + r);
                    }
                    int scale = ByteUtils.readIntLE(r.bytes, r.offset + 1);
                    b.value(new BigDecimal(new BigInteger(r.bytes, r.offset + 5, r.length - 5), scale));
                    return;
                case 'f':
                    if (r.length != 1) {
                        throw new IllegalArgumentException("Can't decode " + r);
                    }
                    b.value(false);
                    return;
                case 'i':
                    b.value(new BigInteger(r.bytes, r.offset + 1, r.length - 1));
                    return;
                case 'j':
                    decodeAndWriteXContent(b, XContentType.JSON, r);
                    return;
                case 's':
                    decodeAndWriteXContent(b, XContentType.SMILE, r);
                    return;
                case 't':
                    if (r.length != 1) {
                        throw new IllegalArgumentException("Can't decode " + r);
                    }
                    b.value(true);
                    return;
                case 'y':
                    decodeAndWriteXContent(b, XContentType.YAML, r);
                    return;
                default:
                    throw new IllegalArgumentException("Can't decode " + r);
            }
        }

        private static void decodeAndWriteXContent(XContentBuilder b, XContentType type, BytesRef r) throws IOException {
            BytesReference ref = new BytesArray(r.bytes, r.offset + 1, r.length - 1);
            try (XContentParser parser = type.xContent().createParser(XContentParserConfiguration.EMPTY, ref.streamInput())) {
                b.copyCurrentStructure(parser);
            }
        }
    }

    private static String name(String fieldName) {
        return fieldName + "._ignore_malformed";
    }

    private static byte[] encode(BigInteger n) {
        byte[] twosCompliment = n.toByteArray();
        byte[] encoded = new byte[1 + twosCompliment.length];
        encoded[0] = 'i';
        System.arraycopy(twosCompliment, 0, encoded, 1, twosCompliment.length);
        return encoded;
    }

    private static byte[] encode(BigDecimal n) {
        byte[] twosCompliment = n.unscaledValue().toByteArray();
        byte[] encoded = new byte[5 + twosCompliment.length];
        encoded[0] = 'd';
        ByteUtils.writeIntLE(n.scale(), encoded, 1);
        System.arraycopy(twosCompliment, 0, encoded, 5, twosCompliment.length);
        return encoded;
    }

    private static byte[] encode(byte[] b) {
        byte[] encoded = new byte[1 + b.length];
        encoded[0] = 'b';
        System.arraycopy(b, 0, encoded, 1, b.length);
        return encoded;
    }

    private static byte[] encode(XContentBuilder builder) throws IOException {
        BytesReference b = BytesReference.bytes(builder);
        byte[] encoded = new byte[1 + b.length()];
        encoded[0] = switch (builder.contentType()) {
            case JSON -> 'j';
            case SMILE -> 's';
            case YAML -> 'y';
            case CBOR -> 'c';
            default -> throw new IllegalArgumentException("unsupported type " + builder.contentType());
        };

        int position = 1;
        BytesRefIterator itr = b.iterator();
        BytesRef ref;
        while ((ref = itr.next()) != null) {
            System.arraycopy(ref.bytes, ref.offset, encoded, position, ref.length);
            position += ref.length;
        }
        assert position == encoded.length;
        return encoded;
    }
}
