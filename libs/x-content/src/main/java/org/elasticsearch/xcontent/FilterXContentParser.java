/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.RestApiVersion;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Delegates every method to the parser returned by the {@link #delegate()} method.
 * To be used extended directly when the delegated parser may dynamically changed.
 * Extend {@link FilterXContentParserWrapper} instead when the delegate is fixed and can be provided at construction time.
 */
public abstract class FilterXContentParser implements XContentParser {

    protected FilterXContentParser() {}

    protected abstract XContentParser delegate();

    @Override
    public XContentType contentType() {
        return delegate().contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        delegate().allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        return delegate().nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        delegate().skipChildren();
    }

    @Override
    public Token currentToken() {
        return delegate().currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return delegate().currentName();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return delegate().map();
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return delegate().mapOrdered();
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        return delegate().mapStrings();
    }

    @Override
    public <T> Map<String, T> map(Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser)
        throws IOException {
        return delegate().map(mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        return delegate().list();
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        return delegate().listOrderedMap();
    }

    @Override
    public String text() throws IOException {
        return delegate().text();
    }

    @Override
    public String textOrNull() throws IOException {
        return delegate().textOrNull();
    }

    public XContentString optimizedText() throws IOException {
        return delegate().optimizedText();
    }

    public XContentString optimizedTextOrNull() throws IOException {
        return delegate().optimizedTextOrNull();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        return delegate().charBufferOrNull();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return delegate().charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return delegate().objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return delegate().objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return delegate().hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return delegate().textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return delegate().textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return delegate().textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return delegate().numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return delegate().numberType();
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        return delegate().shortValue(coerce);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        return delegate().intValue(coerce);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return delegate().longValue(coerce);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        return delegate().floatValue(coerce);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        return delegate().doubleValue(coerce);
    }

    @Override
    public short shortValue() throws IOException {
        return delegate().shortValue();
    }

    @Override
    public int intValue() throws IOException {
        return delegate().intValue();
    }

    @Override
    public long longValue() throws IOException {
        return delegate().longValue();
    }

    @Override
    public float floatValue() throws IOException {
        return delegate().floatValue();
    }

    @Override
    public double doubleValue() throws IOException {
        return delegate().doubleValue();
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        return delegate().isBooleanValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        return delegate().booleanValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return delegate().binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return delegate().getTokenLocation();
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return delegate().namedObject(categoryClass, name, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return delegate().getXContentRegistry();
    }

    @Override
    public boolean isClosed() {
        return delegate().isClosed();
    }

    @Override
    public void close() throws IOException {
        var closeable = delegate();
        if (closeable != null) {
            closeable.close();
        }
    }

    @Override
    public RestApiVersion getRestApiVersion() {
        return delegate().getRestApiVersion();
    }

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return delegate().getDeprecationHandler();
    }
}
