/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.xpack.sql.proto.core.CheckedConsumer;
import org.elasticsearch.xpack.sql.proto.core.CheckedFunction;
import org.elasticsearch.xpack.sql.proto.core.RestApiVersion;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;
import org.elasticsearch.xpack.sql.proto.xcontent.DeprecationHandler;
import org.elasticsearch.xpack.sql.proto.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.sql.proto.xcontent.ToXContent;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentGenerator;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentLocation;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentParser;
import org.elasticsearch.xpack.sql.proto.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Utility class that handles conversion of classes in sql-proto (without any Elasticsearch dependencies)
 * into sql-action which do depend on Elasticsearch (since they are meant to be used in this environment).
 */
final class ProtoShim {

    private static class DelegatingXContentLibDeprecationHandler implements DeprecationHandler {
        private final org.elasticsearch.xcontent.DeprecationHandler esDelegate;

        private static Supplier<org.elasticsearch.xcontent.XContentLocation> fromProto(Supplier<XContentLocation> location) {
            return () -> ProtoShim.fromProto(location.get());
        }

        DelegatingXContentLibDeprecationHandler(org.elasticsearch.xcontent.DeprecationHandler esDelegate) {
            this.esDelegate = esDelegate;
        }

        public void logRenamedField(String parserName, Supplier<XContentLocation> location, String oldName, String currentName) {
            esDelegate.logRenamedField(parserName, fromProto(location), oldName, currentName);
        }

        public void logReplacedField(String parserName, Supplier<XContentLocation> location, String oldName, String replacedName) {
            esDelegate.logReplacedField(parserName, fromProto(location), oldName, replacedName);
        }

        public void logRemovedField(String parserName, Supplier<XContentLocation> location, String removedName) {
            esDelegate.logRemovedField(parserName, fromProto(location), removedName);
        }

        public void logRenamedField(
            String parserName,
            Supplier<XContentLocation> location,
            String oldName,
            String currentName,
            boolean isCompatibleDeprecation
        ) {
            esDelegate.logRenamedField(parserName, fromProto(location), oldName, currentName, isCompatibleDeprecation);
        }

        public void logReplacedField(
            String parserName,
            Supplier<XContentLocation> location,
            String oldName,
            String replacedName,
            boolean isCompatibleDeprecation
        ) {
            esDelegate.logReplacedField(parserName, fromProto(location), oldName, replacedName, isCompatibleDeprecation);
        }

        public void logRemovedField(
            String parserName,
            Supplier<XContentLocation> location,
            String removedName,
            boolean isCompatibleDeprecation
        ) {
            esDelegate.logRemovedField(parserName, fromProto(location), removedName, isCompatibleDeprecation);
        }
    }

    private static class DelegatingXContentLibParser implements XContentParser {

        private final org.elasticsearch.xcontent.XContentParser esXContentParser;

        private static Token toProto(org.elasticsearch.xcontent.XContentParser.Token toProto) {
            if (toProto == null) {
                return null;
            }
            return Token.valueOf(toProto.name());
        }

        private DelegatingXContentLibParser(org.elasticsearch.xcontent.XContentParser esXContentParser) {
            this.esXContentParser = esXContentParser;
        }

        @Override
        public XContentType contentType() {
            return ProtoShim.toProto(esXContentParser.contentType());
        }

        @Override
        public void allowDuplicateKeys(boolean allowDuplicateKeys) {
            esXContentParser.allowDuplicateKeys(allowDuplicateKeys);
        }

        @Override
        public Token nextToken() throws IOException {
            return toProto(esXContentParser.nextToken());
        }

        @Override
        public void skipChildren() throws IOException {
            esXContentParser.skipChildren();
        }

        @Override
        public Token currentToken() {
            return toProto(esXContentParser.currentToken());
        }

        @Override
        public String currentName() throws IOException {
            return esXContentParser.currentName();
        }

        @Override
        public Map<String, Object> map() throws IOException {
            return esXContentParser.map();
        }

        @Override
        public Map<String, Object> mapOrdered() throws IOException {
            return esXContentParser.mapOrdered();
        }

        @Override
        public Map<String, String> mapStrings() throws IOException {
            return esXContentParser.mapStrings();
        }

        public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
            throws IOException {
            return esXContentParser.map(mapFactory, p -> mapValueParser.apply(this));
        }

        @Override
        public List<Object> list() throws IOException {
            return esXContentParser.list();
        }

        @Override
        public List<Object> listOrderedMap() throws IOException {
            return esXContentParser.listOrderedMap();
        }

        @Override
        public String text() throws IOException {
            return esXContentParser.text();
        }

        @Override
        public String textOrNull() throws IOException {
            return esXContentParser.textOrNull();
        }

        @Override
        public CharBuffer charBufferOrNull() throws IOException {
            return esXContentParser.charBufferOrNull();
        }

        @Override
        public CharBuffer charBuffer() throws IOException {
            return esXContentParser.charBuffer();
        }

        @Override
        public Object objectText() throws IOException {
            return esXContentParser.objectText();
        }

        @Override
        public Object objectBytes() throws IOException {
            return esXContentParser.objectBytes();
        }

        @Override
        public boolean hasTextCharacters() {
            return esXContentParser.hasTextCharacters();
        }

        @Override
        public char[] textCharacters() throws IOException {
            return esXContentParser.textCharacters();
        }

        @Override
        public int textLength() throws IOException {
            return esXContentParser.textLength();
        }

        @Override
        public int textOffset() throws IOException {
            return esXContentParser.textOffset();
        }

        @Override
        public Number numberValue() throws IOException {
            return esXContentParser.numberValue();
        }

        @Override
        public NumberType numberType() throws IOException {
            return NumberType.valueOf(esXContentParser.numberType().name());
        }

        @Override
        public short shortValue(boolean coerce) throws IOException {
            return esXContentParser.shortValue(coerce);
        }

        @Override
        public int intValue(boolean coerce) throws IOException {
            return esXContentParser.intValue(coerce);
        }

        @Override
        public long longValue(boolean coerce) throws IOException {
            return esXContentParser.longValue(coerce);
        }

        @Override
        public float floatValue(boolean coerce) throws IOException {
            return esXContentParser.floatValue(coerce);
        }

        @Override
        public double doubleValue(boolean coerce) throws IOException {
            return esXContentParser.doubleValue(coerce);
        }

        @Override
        public short shortValue() throws IOException {
            return esXContentParser.shortValue();
        }

        @Override
        public int intValue() throws IOException {
            return esXContentParser.intValue();
        }

        @Override
        public long longValue() throws IOException {
            return esXContentParser.longValue();
        }

        @Override
        public float floatValue() throws IOException {
            return esXContentParser.floatValue();
        }

        @Override
        public double doubleValue() throws IOException {
            return esXContentParser.doubleValue();
        }

        @Override
        public boolean isBooleanValue() throws IOException {
            return esXContentParser.isBooleanValue();
        }

        @Override
        public boolean booleanValue() throws IOException {
            return esXContentParser.booleanValue();
        }

        @Override
        public byte[] binaryValue() throws IOException {
            return esXContentParser.binaryValue();
        }

        @Override
        public XContentLocation getTokenLocation() {
            return ProtoShim.toProto(esXContentParser.getTokenLocation());
        }

        @Override
        public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
            return esXContentParser.namedObject(categoryClass, name, context);
        }

        @Override
        public NamedXContentRegistry getXContentRegistry() {
            // TODO: this might not work all the time
            return NamedXContentRegistry.EMPTY;
        }

        @Override
        public boolean isClosed() {
            return esXContentParser.isClosed();
        }

        @Override
        public RestApiVersion getRestApiVersion() {
            return ProtoShim.toProto(esXContentParser.getRestApiVersion());
        }

        @Override
        public DeprecationHandler getDeprecationHandler() {
            return new DelegatingXContentLibDeprecationHandler(esXContentParser.getDeprecationHandler());
        }

        @Override
        public void close() throws IOException {
            esXContentParser.close();
        }
    }

    private static class DelegatingXContentLibParam implements ToXContent.Params {
        private final org.elasticsearch.xcontent.ToXContent.Params esDelegate;

        private DelegatingXContentLibParam(org.elasticsearch.xcontent.ToXContent.Params esDelegate) {
            this.esDelegate = esDelegate;
        }

        @Override
        public String param(String key) {
            return esDelegate.param(key);
        }

        @Override
        public String param(String key, String defaultValue) {
            return esDelegate.param(key, defaultValue);
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            return esDelegate.paramAsBoolean(key, defaultValue);
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            return esDelegate.paramAsBoolean(key, defaultValue);
        }
    }

    private static class DelegatingXContentProtoParam implements org.elasticsearch.xcontent.ToXContent.Params {
        private final ToXContent.Params protoDelegate;

        private DelegatingXContentProtoParam(ToXContent.Params protoDelegate) {
            this.protoDelegate = protoDelegate;
        }

        @Override
        public String param(String key) {
            return protoDelegate.param(key);
        }

        @Override
        public String param(String key, String defaultValue) {
            return protoDelegate.param(key, defaultValue);
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            return protoDelegate.paramAsBoolean(key, defaultValue);
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            return protoDelegate.paramAsBoolean(key, defaultValue);
        }
    }

    private static class DelegatingXContentLibToXContent implements ToXContent {
        private final org.elasticsearch.xcontent.ToXContent esDelegate;

        private DelegatingXContentLibToXContent(org.elasticsearch.xcontent.ToXContent esDelegate) {
            this.esDelegate = esDelegate;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            org.elasticsearch.xcontent.XContentBuilder xContentBuilder = null;
            if (builder instanceof DelegatingXContentLibBuilder) {
                esDelegate.toXContent(((DelegatingXContentLibBuilder) builder).esDelegate, fromProto(params));
            } else {
                throw new IllegalArgumentException("Expecting a wrapped x-content builder");
            }
            return builder;
        }

        @Override
        public boolean isFragment() {
            return esDelegate.isFragment();
        }
    }

    private static class DelegatingXContentLibGenerator implements XContentGenerator {

        private final org.elasticsearch.xcontent.XContentGenerator esDelegate;

        private DelegatingXContentLibGenerator(org.elasticsearch.xcontent.XContentGenerator esDelegate) {
            this.esDelegate = esDelegate;
        }

        @Override
        public JsonGenerator getLowLevelGenerator() {
            return null;
        }

        @Override
        public XContentType contentType() {
            return toProto(esDelegate.contentType());
        }

        @Override
        public void usePrettyPrint() {
            esDelegate.usePrettyPrint();
        }

        @Override
        public boolean isPrettyPrint() {
            return esDelegate.isPrettyPrint();
        }

        @Override
        public void usePrintLineFeedAtEnd() {
            esDelegate.usePrintLineFeedAtEnd();
        }

        @Override
        public void writeStartObject() throws IOException {
            esDelegate.writeStartObject();
        }

        @Override
        public void writeEndObject() throws IOException {
            esDelegate.writeEndObject();
        }

        @Override
        public void writeStartArray() throws IOException {
            esDelegate.writeStartArray();
        }

        @Override
        public void writeEndArray() throws IOException {
            esDelegate.writeEndArray();
        }

        @Override
        public void writeFieldName(String name) throws IOException {
            esDelegate.writeFieldName(name);
        }

        @Override
        public void writeNull() throws IOException {
            esDelegate.writeNull();
        }

        @Override
        public void writeNullField(String name) throws IOException {
            esDelegate.writeNullField(name);
        }

        @Override
        public void writeBooleanField(String name, boolean value) throws IOException {
            esDelegate.writeBooleanField(name, value);
        }

        @Override
        public void writeBoolean(boolean value) throws IOException {
            esDelegate.writeBoolean(value);
        }

        @Override
        public void writeNumberField(String name, double value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeNumber(double value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumberField(String name, float value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeNumber(float value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumberField(String name, int value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeNumber(int value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumberField(String name, long value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeNumber(long value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumber(short value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumber(BigInteger value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumberField(String name, BigInteger value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeNumber(BigDecimal value) throws IOException {
            esDelegate.writeNumber(value);
        }

        @Override
        public void writeNumberField(String name, BigDecimal value) throws IOException {
            esDelegate.writeNumberField(name, value);
        }

        @Override
        public void writeStringField(String name, String value) throws IOException {
            esDelegate.writeStringField(name, value);
        }

        @Override
        public void writeString(String value) throws IOException {
            esDelegate.writeString(value);
        }

        @Override
        public void writeString(char[] text, int offset, int len) throws IOException {
            esDelegate.writeString(text, offset, len);
        }

        @Override
        public void writeUTF8String(byte[] value, int offset, int length) throws IOException {
            esDelegate.writeUTF8String(value, offset, length);
        }

        @Override
        public void writeBinaryField(String name, byte[] value) throws IOException {
            esDelegate.writeBinaryField(name, value);
        }

        @Override
        public void writeBinary(byte[] value) throws IOException {
            esDelegate.writeBinary(value);
        }

        @Override
        public void writeBinary(byte[] value, int offset, int length) throws IOException {
            esDelegate.writeBinary(value, offset, length);
        }

        @Override
        @Deprecated
        public void writeRawField(String name, InputStream value) throws IOException {
            esDelegate.writeRawField(name, value);
        }

        @Override
        public void writeRawField(String name, InputStream value, XContentType xContentType) throws IOException {
            esDelegate.writeRawField(name, value, fromProto(xContentType));
        }

        @Override
        public void writeRawValue(InputStream value, XContentType xContentType) throws IOException {
            esDelegate.writeRawValue(value, fromProto(xContentType));
        }

        @Override
        public void copyCurrentStructure(XContentParser parser) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeDirectField(String name, CheckedConsumer<OutputStream, IOException> writer) throws IOException {
            esDelegate.writeDirectField(name, writer::accept);
        }

        @Override
        public boolean isClosed() {
            return esDelegate.isClosed();
        }

        @Override
        public void close() throws IOException {
            esDelegate.close();
        }

        @Override
        public void flush() throws IOException {
            esDelegate.flush();
        }
    }

    private static class DelegatingXContentLibBuilder extends XContentBuilder {

        private final org.elasticsearch.xcontent.XContentBuilder esDelegate;

        DelegatingXContentLibBuilder(org.elasticsearch.xcontent.XContentBuilder esDelegate) throws IOException {
            super(
                new DelegatingXContentLibGenerator(esDelegate.generator()),
                esDelegate.getOutputStream(),
                toProto(esDelegate.getRestApiVersion())
            );
            this.esDelegate = esDelegate;
        }
    }

    private ProtoShim() {}

    //
    // Core classes
    //
    static org.elasticsearch.core.TimeValue fromProto(TimeValue fromProto) {
        if (fromProto == null) {
            return null;
        }
        return new org.elasticsearch.core.TimeValue(fromProto.duration(), fromProto.timeUnit());
    }

    static TimeValue toProto(org.elasticsearch.core.TimeValue toProto) {
        if (toProto == null) {
            return null;
        }
        return new TimeValue(toProto.duration(), toProto.timeUnit());
    }

    //
    // XContent classes
    //

    static org.elasticsearch.xcontent.XContentType fromProto(XContentType fromProto) {
        switch (fromProto) {
            case JSON:
                return org.elasticsearch.xcontent.XContentType.JSON;
            case CBOR:
                return org.elasticsearch.xcontent.XContentType.CBOR;
            default:
                throw new IllegalArgumentException("Unsupported content type " + fromProto);
        }
    }

    static XContentType toProto(org.elasticsearch.xcontent.XContentType toProto) {
        switch (toProto) {
            case JSON:
                return XContentType.JSON;
            case CBOR:
                return XContentType.CBOR;
            default:
                throw new IllegalArgumentException("Unsupported content type " + toProto);
        }
    }

    static org.elasticsearch.core.RestApiVersion fromProto(RestApiVersion fromProto) {
        return org.elasticsearch.core.RestApiVersion.valueOf(fromProto.name());
    }

    static RestApiVersion toProto(org.elasticsearch.core.RestApiVersion toProto) {
        return RestApiVersion.valueOf(toProto.name());
    }

    static XContentParser toProto(org.elasticsearch.xcontent.XContentParser toProto) {
        if (toProto == null) {
            return null;
        }
        if (toProto instanceof org.elasticsearch.xcontent.json.JsonXContentParser == false) {
            throw new IllegalArgumentException("Unsupported XContentParser " + toProto);
        }

        return new DelegatingXContentLibParser(toProto);
    }

    static org.elasticsearch.xcontent.XContentLocation fromProto(XContentLocation fromProto) {
        if (fromProto == null) {
            return null;
        }
        return new org.elasticsearch.xcontent.XContentLocation(fromProto.lineNumber, fromProto.columnNumber);
    }

    static XContentLocation toProto(org.elasticsearch.xcontent.XContentLocation toProto) {
        if (toProto == null) {
            return null;
        }
        return new XContentLocation(toProto.lineNumber, toProto.columnNumber);
    }

    static org.elasticsearch.xcontent.XContentBuilder fromProto(
        ToXContent fromProto,
        org.elasticsearch.xcontent.XContentBuilder builder,
        org.elasticsearch.xcontent.ToXContent.Params params
    ) throws IOException {
        XContentBuilder protoBuilder = ProtoShim.toProto(builder);
        ToXContent.Params protoParam = ProtoShim.toProto(params);

        fromProto.toXContent(protoBuilder, protoParam);
        return builder;
    }

    static XContentBuilder toProto(org.elasticsearch.xcontent.XContentBuilder toProto) throws IOException {
        if (toProto == null) {
            return null;
        }
        // to deal with fragments and other internal settings which might not be available outside
        // pass the builder as a delegated generator
        return new DelegatingXContentLibBuilder(toProto);
    }

    static org.elasticsearch.xcontent.ToXContent.Params fromProto(ToXContent.Params fromProto) {
        if (fromProto == null) {
            return null;
        }
        if (fromProto == ToXContent.EMPTY_PARAMS) {
            return org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
        }
        return new DelegatingXContentProtoParam(fromProto);
    }

    static ToXContent.Params toProto(org.elasticsearch.xcontent.ToXContent.Params toProto) {
        if (toProto == null) {
            return null;
        }
        if (toProto == org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS) {
            return ToXContent.EMPTY_PARAMS;
        }
        return new DelegatingXContentLibParam(toProto);
    }

    static ToXContent toProto(org.elasticsearch.xcontent.ToXContent toProto) {
        if (toProto == null) {
            return null;
        }
        if (toProto.isFragment()) {
            throw new IllegalArgumentException("XContent fragments not supported");
        }
        return new DelegatingXContentLibToXContent(toProto);
    }
}
