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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Helper class for processing field data of any type, as provided by the {@link XContentParser}.
 */
public final class XContentDataHelper {
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
    static StoredField storedField(String name, XContentParser parser) throws IOException {
        return (StoredField) processToken(parser, typeUtils -> typeUtils.buildStoredField(name, parser));
    }

    /**
     * Build a {@link StoredField} for the value provided in a {@link XContentBuilder}.
     */
    static StoredField storedField(String name, XContentBuilder builder) throws IOException {
        return new StoredField(name, TypeUtils.encode(builder));
    }

    /**
     * Build a {@link BytesRef} wrapping a byte array containing an encoded form
     * the value on which the parser is currently positioned.
     */
    static BytesRef encodeToken(XContentParser parser) throws IOException {
        return new BytesRef((byte[]) processToken(parser, (typeUtils) -> typeUtils.encode(parser)));
    }

    /**
     * Build a {@link BytesRef} wrapping a byte array containing an encoded form
     * of the passed XContentBuilder contents.
     */
    public static BytesRef encodeXContentBuilder(XContentBuilder builder) throws IOException {
        return new BytesRef(TypeUtils.encode(builder));
    }

    /**
     * Decode the value in the passed {@link BytesRef} and add it as a value to the
     * passed build. The assumption is that the passed value has encoded using the function
     * {@link #encodeToken(XContentParser)} above.
     */
    static void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
        switch ((char) r.bytes[r.offset]) {
            case BINARY_ENCODING -> TypeUtils.EMBEDDED_OBJECT.decodeAndWrite(b, r);
            case CBOR_OBJECT_ENCODING, JSON_OBJECT_ENCODING, YAML_OBJECT_ENCODING, SMILE_OBJECT_ENCODING -> {
                TypeUtils.START.decodeAndWrite(b, r);
            }
            case BIG_DECIMAL_ENCODING -> TypeUtils.BIG_DECIMAL.decodeAndWrite(b, r);
            case FALSE_ENCODING, TRUE_ENCODING -> TypeUtils.BOOLEAN.decodeAndWrite(b, r);
            case BIG_INTEGER_ENCODING -> TypeUtils.BIG_INTEGER.decodeAndWrite(b, r);
            case STRING_ENCODING -> TypeUtils.STRING.decodeAndWrite(b, r);
            case INTEGER_ENCODING -> TypeUtils.INTEGER.decodeAndWrite(b, r);
            case LONG_ENCODING -> TypeUtils.LONG.decodeAndWrite(b, r);
            case DOUBLE_ENCODING -> TypeUtils.DOUBLE.decodeAndWrite(b, r);
            case FLOAT_ENCODING -> TypeUtils.FLOAT.decodeAndWrite(b, r);
            case NULL_ENCODING -> TypeUtils.NULL.decodeAndWrite(b, r);
            default -> throw new IllegalArgumentException("Can't decode " + r);
        }
    }

    /**
     * Writes encoded values to provided builder. If there are multiple values they are merged into
     * a single resulting array.
     * @param b destination
     * @param fieldName name of the field that is written
     * @param encodedParts subset of field data encoded using methods of this class. Can contain arrays which will be flattened.
     * @throws IOException
     */
    static void writeMerged(XContentBuilder b, String fieldName, List<BytesRef> encodedParts) throws IOException {
        if (encodedParts.isEmpty()) {
            return;
        }

        if (encodedParts.size() == 1) {
            b.field(fieldName);
            XContentDataHelper.decodeAndWrite(b, encodedParts.get(0));
            return;
        }

        b.startArray(fieldName);

        for (var encodedValue : encodedParts) {
            Optional<XContentType> encodedXContentType = switch ((char) encodedValue.bytes[encodedValue.offset]) {
                case CBOR_OBJECT_ENCODING, JSON_OBJECT_ENCODING, YAML_OBJECT_ENCODING, SMILE_OBJECT_ENCODING -> Optional.of(
                    getXContentType(encodedValue)
                );
                default -> Optional.empty();
            };
            if (encodedXContentType.isEmpty()) {
                // This is a plain value, we can just write it
                XContentDataHelper.decodeAndWrite(b, encodedValue);
            } else {
                // Encoded value could be an array which needs to be flattened
                // since we are already inside an array.
                try (
                    XContentParser parser = encodedXContentType.get()
                        .xContent()
                        .createParser(
                            XContentParserConfiguration.EMPTY,
                            encodedValue.bytes,
                            encodedValue.offset + 1,
                            encodedValue.length - 1
                        )
                ) {
                    if (parser.currentToken() == null) {
                        parser.nextToken();
                    }

                    // It's an array, we will flatten it.
                    if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            b.copyCurrentStructure(parser);
                        }
                    } else {
                        // It is a single complex structure (an object), write it as is.
                        b.copyCurrentStructure(parser);
                    }
                }
            }
        }

        b.endArray();
    }

    /**
     * Returns the {@link XContentType} to use for creating an XContentBuilder to decode the passed value.
     */
    public static XContentType getXContentType(BytesRef r) {
        return switch ((char) r.bytes[r.offset]) {
            case JSON_OBJECT_ENCODING -> XContentType.JSON;
            case YAML_OBJECT_ENCODING -> XContentType.YAML;
            case SMILE_OBJECT_ENCODING -> XContentType.SMILE;
            default -> XContentType.CBOR;  // CBOR can parse all other encoded types.
        };
    }

    /**
     * Stores the current parser structure (subtree) to an {@link XContentBuilder} and returns it, along with a
     * {@link DocumentParserContext} wrapping it that can be used to reparse the subtree.
     * The parser of the original context is also advanced to the end of the current structure (subtree) as a side effect.
     */
    static Tuple<DocumentParserContext, XContentBuilder> cloneSubContext(DocumentParserContext context) throws IOException {
        var tuple = cloneSubContextParserConfiguration(context);
        return Tuple.tuple(cloneDocumentParserContext(context, tuple.v1(), tuple.v2()), tuple.v2());
    }

    /**
     * Initializes a {@link XContentParser} with the current parser structure (subtree) and returns it, along with a
     * {@link DocumentParserContext} wrapping the subtree that can be used to reparse it.
     * The parser of the original context is also advanced to the end of the current structure (subtree) as a side effect.
     */
    static Tuple<DocumentParserContext, XContentParser> cloneSubContextWithParser(DocumentParserContext context) throws IOException {
        Tuple<XContentParserConfiguration, XContentBuilder> tuple = cloneSubContextParserConfiguration(context);
        XContentParser parser = XContentHelper.createParserNotCompressed(
            tuple.v1(),
            BytesReference.bytes(tuple.v2()),
            context.parser().contentType()
        );
        assert parser.currentToken() == null;
        parser.nextToken();
        return Tuple.tuple(cloneDocumentParserContext(context, tuple.v1(), tuple.v2()), parser);
    }

    private static Tuple<XContentParserConfiguration, XContentBuilder> cloneSubContextParserConfiguration(DocumentParserContext context)
        throws IOException {
        XContentParser parser = context.parser();
        XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent());
        builder.copyCurrentStructure(parser);

        XContentParserConfiguration configuration = XContentParserConfiguration.EMPTY.withRegistry(parser.getXContentRegistry())
            .withDeprecationHandler(parser.getDeprecationHandler())
            .withRestApiVersion(parser.getRestApiVersion());
        return Tuple.tuple(configuration, builder);
    }

    private static DocumentParserContext cloneDocumentParserContext(
        DocumentParserContext context,
        XContentParserConfiguration configuration,
        XContentBuilder builder
    ) throws IOException {
        DocumentParserContext subcontext = context.switchParser(
            XContentHelper.createParserNotCompressed(configuration, BytesReference.bytes(builder), context.parser().contentType())
        );
        subcontext.setClonedSource();  // Avoids double-storing parts of the source for the same parser subtree.
        subcontext.parser().nextToken();
        return subcontext;
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
            case VALUE_NULL -> visitor.apply(TypeUtils.NULL);
            case VALUE_EMBEDDED_OBJECT -> visitor.apply(TypeUtils.EMBEDDED_OBJECT);
            case START_OBJECT, START_ARRAY -> visitor.apply(TypeUtils.START);
            default -> throw new IllegalArgumentException("synthetic _source doesn't support malformed objects");
        };
    }

    private static final char STRING_ENCODING = 'S';
    private static final char INTEGER_ENCODING = 'I';
    private static final char LONG_ENCODING = 'L';
    private static final char DOUBLE_ENCODING = 'D';
    private static final char FLOAT_ENCODING = 'F';
    private static final char BIG_INTEGER_ENCODING = 'i';
    private static final char BIG_DECIMAL_ENCODING = 'd';
    private static final char FALSE_ENCODING = 'f';
    private static final char TRUE_ENCODING = 't';
    private static final char BINARY_ENCODING = 'b';
    private static final char NULL_ENCODING = 'n';
    private static final char CBOR_OBJECT_ENCODING = 'c';
    private static final char JSON_OBJECT_ENCODING = 'j';
    private static final char YAML_OBJECT_ENCODING = 'y';
    private static final char SMILE_OBJECT_ENCODING = 's';

    private enum TypeUtils {
        STRING(STRING_ENCODING) {
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
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(new BytesRef(r.bytes, r.offset + 1, r.length - 1).utf8ToString());
            }
        },
        INTEGER(INTEGER_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.intValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[5];
                bytes[0] = getEncoding();
                ByteUtils.writeIntLE(parser.intValue(), bytes, 1);
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readIntLE(r.bytes, 1 + r.offset));
            }
        },
        LONG(LONG_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.longValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[9];
                bytes[0] = getEncoding();
                ByteUtils.writeLongLE(parser.longValue(), bytes, 1);
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readLongLE(r.bytes, 1 + r.offset));
            }
        },
        DOUBLE(DOUBLE_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.doubleValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[9];
                bytes[0] = getEncoding();
                ByteUtils.writeDoubleLE(parser.doubleValue(), bytes, 1);
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readDoubleLE(r.bytes, 1 + r.offset));
            }
        },
        FLOAT(FLOAT_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, parser.floatValue());
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[5];
                bytes[0] = getEncoding();
                ByteUtils.writeFloatLE(parser.floatValue(), bytes, 1);
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(ByteUtils.readFloatLE(r.bytes, 1 + r.offset));
            }
        },
        BIG_INTEGER(BIG_INTEGER_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = encode((BigInteger) parser.numberValue(), getEncoding());
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(new BigInteger(r.bytes, r.offset + 1, r.length - 1));
            }
        },
        BIG_DECIMAL(BIG_DECIMAL_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = encode((BigDecimal) parser.numberValue(), getEncoding());
                assertValidEncoding(bytes);
                return bytes;
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
        BOOLEAN(new Character[] { TRUE_ENCODING, FALSE_ENCODING }) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[] { parser.booleanValue() ? (byte) 't' : (byte) 'f' };
                assertValidEncoding(bytes);
                return bytes;
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
        NULL(NULL_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = new byte[] { getEncoding() };
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.nullValue();
            }
        },
        EMBEDDED_OBJECT(BINARY_ENCODING) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser.binaryValue()));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                byte[] bytes = encode(parser.binaryValue());
                assertValidEncoding(bytes);
                return bytes;
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                b.value(r.bytes, r.offset + 1, r.length - 1);
            }
        },
        START(new Character[] { CBOR_OBJECT_ENCODING, JSON_OBJECT_ENCODING, YAML_OBJECT_ENCODING, SMILE_OBJECT_ENCODING }) {
            @Override
            StoredField buildStoredField(String name, XContentParser parser) throws IOException {
                return new StoredField(name, encode(parser));
            }

            @Override
            byte[] encode(XContentParser parser) throws IOException {
                try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                    builder.copyCurrentStructure(parser);
                    byte[] bytes = encode(builder);
                    assertValidEncoding(bytes);
                    return bytes;
                }
            }

            @Override
            void decodeAndWrite(XContentBuilder b, BytesRef r) throws IOException {
                switch ((char) r.bytes[r.offset]) {
                    case CBOR_OBJECT_ENCODING -> decodeAndWriteXContent(b, XContentType.CBOR, r);
                    case JSON_OBJECT_ENCODING -> decodeAndWriteXContent(b, XContentType.JSON, r);
                    case SMILE_OBJECT_ENCODING -> decodeAndWriteXContent(b, XContentType.SMILE, r);
                    case YAML_OBJECT_ENCODING -> decodeAndWriteXContent(b, XContentType.YAML, r);
                    default -> throw new IllegalArgumentException("Can't decode " + r);
                }
            }
        };

        TypeUtils(char encoding) {
            this.encoding = new Character[] { encoding };
        }

        TypeUtils(Character[] encoding) {
            this.encoding = encoding;
        }

        byte getEncoding() {
            assert encoding.length == 1;
            return (byte) encoding[0].charValue();
        }

        void assertValidEncoding(byte[] encodedValue) {
            assert Arrays.asList(encoding).contains((char) encodedValue[0]);
        }

        final Character[] encoding;

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
                case JSON -> JSON_OBJECT_ENCODING;
                case SMILE -> SMILE_OBJECT_ENCODING;
                case YAML -> YAML_OBJECT_ENCODING;
                case CBOR -> CBOR_OBJECT_ENCODING;
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
