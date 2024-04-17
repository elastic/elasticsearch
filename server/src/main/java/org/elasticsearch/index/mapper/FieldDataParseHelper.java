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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Helper class for processing field data of any type, as provided by the {@link XContentParser}.
 */
public final class FieldDataParseHelper {
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
    public static StoredField storedField(String name, XContentParser parser) throws IOException {
        return (StoredField) processToken(parser, typeUtils -> typeUtils.buildStoredField(name, parser));
    }

    /**
     * Build a {@link BytesRef} wrapping a byte array containing an encoded form
     * the value on which the parser is currently positioned.
     */
    public static BytesRef encodeToken(XContentParser parser) throws IOException {
        return new BytesRef((byte[]) processToken(parser, (typeUtils) -> typeUtils.encode(parser)));
    }

    /**
     * Decode the value in the passed {@link BytesRef} and add it as a value to the
     * passed build. The assumption is that the passed value has encoded using the function
     * {@link #encodeToken(XContentParser)} above.
     */
    public static void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
        switch (r.bytes[r.offset]) {
            case 'b' -> TypeUtils.EMBEDDED_OBJECT.decodeAndWrite(b, r);
            case 'c', 'j', 's', 'y' -> TypeUtils.START.decodeAndWrite(b, r);
            case 'd' -> TypeUtils.BIG_DECIMAL.decodeAndWrite(b, r);
            case 'f', 't' -> TypeUtils.BOOLEAN.decodeAndWrite(b, r);
            case 'i' -> TypeUtils.BIG_INTEGER.decodeAndWrite(b, r);
            case 'S' -> TypeUtils.STRING.decodeAndWrite(b, r);
            case 'I' -> TypeUtils.INTEGER.decodeAndWrite(b, r);
            case 'L' -> TypeUtils.LONG.decodeAndWrite(b, r);
            case 'D' -> TypeUtils.DOUBLE.decodeAndWrite(b, r);
            case 'F' -> TypeUtils.FLOAT.decodeAndWrite(b, r);
            default -> throw new IllegalArgumentException("Can't decode " + r);
        }
    }

    private static Object processToken(XContentParser parser, CheckedFunction<TypeUtils, Object, IOException> visitor) throws IOException {
        return switch (parser.currentToken()) {
            case VALUE_STRING -> visitor.apply(TypeUtils.STRING);
            case VALUE_NUMBER -> switch (parser.numberType()) {
                case INT -> visitor.apply(TypeUtils.INTEGER);
                case LONG -> visitor.apply(TypeUtils.LONG);
                case DOUBLE -> visitor.apply(TypeUtils.DOUBLE);
                case FLOAT -> visitor.apply(TypeUtils.FLOAT);
                case BIG_INTEGER -> visitor.apply(TypeUtils.BIG_INTEGER);
                case BIG_DECIMAL -> visitor.apply(TypeUtils.BIG_DECIMAL);
            };
            case VALUE_BOOLEAN -> visitor.apply(TypeUtils.BOOLEAN);
            case VALUE_EMBEDDED_OBJECT -> visitor.apply(TypeUtils.EMBEDDED_OBJECT);
            case START_OBJECT, START_ARRAY -> visitor.apply(TypeUtils.START);
            default -> throw new IllegalArgumentException("synthetic _source doesn't support malformed objects");
        };
    }

    private enum TypeUtils {
        STRING('S') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.text());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] text = parser.text().getBytes(StandardCharsets.UTF_8);
                byte[] bytes = new byte[text.length + 1];
                bytes[0] = getEncoding();
                System.arraycopy(text, 0, bytes, 1, text.length);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(new BytesRef(r.bytes, r.offset + 1, r.length - 1).utf8ToString());
            }
        },
        INTEGER('I') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.intValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[5];
                bytes[0] = getEncoding();
                ByteUtils.writeIntLE(parser.intValue(), bytes, 1);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readIntLE(r.bytes, 1 + r.offset));
            }
        },
        LONG('L') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.longValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[9];
                bytes[0] = getEncoding();
                ByteUtils.writeLongLE(parser.longValue(), bytes, 1);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readLongLE(r.bytes, 1 + r.offset));
            }
        },
        DOUBLE('D') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.doubleValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[9];
                bytes[0] = getEncoding();
                ByteUtils.writeDoubleLE(parser.doubleValue(), bytes, 1);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readDoubleLE(r.bytes, 1 + r.offset));
            }
        },
        FLOAT('F') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.floatValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[5];
                bytes[0] = getEncoding();
                ByteUtils.writeFloatLE(parser.floatValue(), bytes, 1);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readFloatLE(r.bytes, 1 + r.offset));
            }
        },
        BIG_INTEGER('i') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                return encode((BigInteger) parser.numberValue(), getEncoding());
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(new BigInteger(r.bytes, r.offset + 1, r.length - 1));
            }
        },
        BIG_DECIMAL('d') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                return encode((BigDecimal) parser.numberValue(), getEncoding());
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                if (r.length < 5) {
                    throw new IllegalArgumentException("Can't decode " + r);
                }
                int scale = ByteUtils.readIntLE(r.bytes, r.offset + 1);
                b.value(new BigDecimal(new BigInteger(r.bytes, r.offset + 5, r.length - 5), scale));
            }
        },
        BOOLEAN('B') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                return new byte[] { parser.booleanValue() ? (byte) 't' : (byte) 'f' };
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                if (r.length != 1) {
                    throw new IllegalArgumentException("Can't decode " + r);
                }
                assert r.bytes[r.offset] == 't' || r.bytes[r.offset] == 'f' : r.bytes[r.offset];
                b.value(r.bytes[r.offset] == 't');
            }
        },
        EMBEDDED_OBJECT('b') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser.binaryValue()));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                return encode(parser.binaryValue());
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(r.bytes, r.offset + 1, r.length - 1);
            }
        },
        START('O') {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                    builder.copyCurrentStructure(parser);
                    return encode(builder);
                }
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                switch (r.bytes[r.offset]) {
                    case 'c' -> decodeAndWriteXContent(b, XContentType.CBOR, r);
                    case 'j' -> decodeAndWriteXContent(b, XContentType.JSON, r);
                    case 's' -> decodeAndWriteXContent(b, XContentType.SMILE, r);
                    case 'y' -> decodeAndWriteXContent(b, XContentType.YAML, r);
                    default -> throw new IllegalArgumentException("Can't decode " + r);
                }
            }
        };

        TypeUtils(char encoding) {
            this.encoding = encoding;
        }

        byte getEncoding() {
            return (byte) encoding;
        }

        final char encoding;

        abstract StoredField buildStoredField(String name, XContentParser parser) throws IOException;

        abstract byte[] encode(XContentParser parser) throws IOException;

        abstract void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException;

        static byte[] encode(BigInteger n, Byte encoding) throws IOException {
            byte[] twosCompliment = n.toByteArray();
            byte[] encoded = new byte[1 + twosCompliment.length];
            encoded[0] = encoding;
            System.arraycopy(twosCompliment, 0, encoded, 1, twosCompliment.length);
            return encoded;
        }

        static byte[] encode(BigDecimal n, Byte encoding) {
            byte[] twosCompliment = n.unscaledValue().toByteArray();
            byte[] encoded = new byte[5 + twosCompliment.length];
            encoded[0] = 'd';
            ByteUtils.writeIntLE(n.scale(), encoded, 1);
            System.arraycopy(twosCompliment, 0, encoded, 5, twosCompliment.length);
            return encoded;
        }

        static byte[] encode(byte[] b) {
            byte[] encoded = new byte[1 + b.length];
            encoded[0] = 'b';
            System.arraycopy(b, 0, encoded, 1, b.length);
            return encoded;
        }

        static byte[] encode(XContentBuilder builder) throws IOException {
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

        static void decodeAndWriteXContent(XContentBuilder b, XContentType type, BytesRef r) throws IOException {
            try (
                XContentParser parser = type.xContent().createParser(XContentParserConfiguration.EMPTY, r.bytes, r.offset + 1, r.length - 1)
            ) {
                b.copyCurrentStructure(parser);
            }
        }
    }
}
