/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Filters an existing XContentParser by using a delegate
 */
public abstract class FilterXContentParser implements XContentParser {

    protected final XContentParser in;

    protected FilterXContentParser(XContentParser in) {
        this.in = in;
    }

    @Override
    public XContentType contentType() {
        return in.contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        in.allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        return in.nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        in.skipChildren();
    }

    @Override
    public Token currentToken() {
        return in.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return in.currentName();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return in.map();
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return in.mapOrdered();
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        return in.mapStrings();
    }

    @Override
    public <T> Map<String, T> map(
        Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException {
        return in.map(mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        return in.list();
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        return in.listOrderedMap();
    }

    @Override
    public String text() throws IOException {
        return in.text();
    }

    @Override
    public String textOrNull() throws IOException {
        return in.textOrNull();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        return in.charBufferOrNull();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return in.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return in.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return in.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return in.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return in.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return in.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return in.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return in.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return in.numberType();
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        return in.shortValue(coerce);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        return in.intValue(coerce);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return in.longValue(coerce);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        return in.floatValue(coerce);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        return in.doubleValue(coerce);
    }

    @Override
    public short shortValue() throws IOException {
        return in.shortValue();
    }

    @Override
    public int intValue() throws IOException {
        return in.intValue();
    }

    @Override
    public long longValue() throws IOException {
        return in.longValue();
    }

    @Override
    public float floatValue() throws IOException {
        return in.floatValue();
    }

    @Override
    public double doubleValue() throws IOException {
        return in.doubleValue();
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        return in.isBooleanValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        return in.booleanValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return in.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return in.getTokenLocation();
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return in.namedObject(categoryClass, name, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return in.getXContentRegistry();
    }

    @Override
    public boolean isClosed() {
        return in.isClosed();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public RestApiVersion getRestApiVersion() {
        return in.getRestApiVersion();
    }

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return in.getDeprecationHandler();
    }
}
