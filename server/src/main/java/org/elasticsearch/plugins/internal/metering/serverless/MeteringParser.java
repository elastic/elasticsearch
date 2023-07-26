/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering.serverless;

import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class MeteringParser extends AbstractXContentParser {
    private final XContentParser delegate;
    private final AtomicLong counter;

    private void charge(long value) {
        counter.addAndGet(value);
    }

    public MeteringParser(XContentParser xContentParser, AtomicLong counter) {
        super(xContentParser.getXContentRegistry(), xContentParser.getDeprecationHandler(), xContentParser.getRestApiVersion());
        this.delegate = xContentParser;
        this.counter = counter;
    }

    // generated of AbstractXContentParser methods delegation below
    @Override
    public XContentType contentType() {
        return delegate.contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        delegate.allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        charge(1);
        return delegate.nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        delegate.skipChildren();
    }

    @Override
    public Token currentToken() {
        return delegate.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return delegate.currentName();
    }

    @Override
    public String text() throws IOException {
        return delegate.text();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return delegate.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return delegate.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return delegate.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return delegate.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return delegate.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return delegate.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return delegate.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return delegate.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return delegate.numberType();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return delegate.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return delegate.getTokenLocation();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return delegate.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return delegate.shortValue();
    }

    @Override
    protected int doIntValue() throws IOException {

        return delegate.intValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return delegate.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        return delegate.floatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        return delegate.doubleValue();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void close() throws IOException {

        delegate.close();
    }
}
