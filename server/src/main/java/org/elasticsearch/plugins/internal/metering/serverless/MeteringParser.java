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
    private final XContentParser x;
    private AtomicLong counter = new AtomicLong();

    private void charge(long value){
        counter.addAndGet(value);
    }
    public AtomicLong getCounter() {
        return counter;
    }

    public MeteringParser(XContentParser xContentParser) {
        super(xContentParser.getXContentRegistry(),xContentParser.getDeprecationHandler(),xContentParser.getRestApiVersion());
        this.x = xContentParser;
    }

    @Override
    public XContentType contentType() {
        return x.contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        x.allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        charge(1);
        return x.nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        x.skipChildren();
    }

    @Override
    public Token currentToken() {
        return x.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return x.currentName();
    }

    @Override
    public String text() throws IOException {
        return x.text();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return x.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return x.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return x.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return x.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return x.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return x.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return x.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return x.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return x.numberType();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return x.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return x.getTokenLocation();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return x.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return x.shortValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return x.intValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return x.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        return x.floatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        return x.doubleValue();
    }

    @Override
    public boolean isClosed() {
        return x.isClosed();
    }

    @Override
    public void close() throws IOException {

        x.close();
    }
}
