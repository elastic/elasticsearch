/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.support.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;

import java.io.IOException;
import java.nio.CharBuffer;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A xcontent parser that is used by watcher. This is a special parser that is
 * aware of watcher services. In particular, it's aware of the used {@link Clock}
 * and the CryptoService. The former (clock) may be used when the current time
 * is required during the parse phase of construct. The latter (crypto service) is used
 * to encode secret values (e.g. passwords, security tokens, etc..) to {@link Secret}s.
 * {@link Secret}s are encrypted values that are stored in memory and are decrypted
 * on demand when needed.
 */
public class WatcherXContentParser implements XContentParser {

    public static final String REDACTED_PASSWORD = "::es_redacted::";

    public static Secret secretOrNull(XContentParser parser) throws IOException {
        String text = parser.textOrNull();
        if (text == null) {
            return null;
        }

        char[] chars = text.toCharArray();
        boolean isEncryptedAlready = text.startsWith(CryptoService.ENCRYPTED_TEXT_PREFIX);
        if (isEncryptedAlready) {
            return new Secret(chars);
        }

        if (parser instanceof WatcherXContentParser) {
            WatcherXContentParser watcherParser = (WatcherXContentParser) parser;
            if (REDACTED_PASSWORD.equals(text)) {
                if (watcherParser.allowRedactedPasswords) {
                    return null;
                } else {
                    throw new ElasticsearchParseException("found redacted password in field [{}]", parser.currentName());
                }
            } else if (watcherParser.cryptoService != null) {
                return new Secret(watcherParser.cryptoService.encrypt(chars));
            }
        }

        return new Secret(chars);
    }

    private final ZonedDateTime parseTime;
    private final XContentParser parser;
    @Nullable private final CryptoService cryptoService;
    private final boolean allowRedactedPasswords;

    public WatcherXContentParser(XContentParser parser, ZonedDateTime parseTime, @Nullable CryptoService cryptoService,
                                 boolean allowRedactedPasswords) {
        this.parseTime = parseTime;
        this.parser = parser;
        this.cryptoService = cryptoService;
        this.allowRedactedPasswords = allowRedactedPasswords;
    }

    public ZonedDateTime getParseDateTime() { return parseTime; }

    @Override
    public XContentType contentType() {
        return parser.contentType();
    }

    @Override
    public Token nextToken() throws IOException {
        return parser.nextToken();
    }

    @Override
    public void skipChildren() throws IOException {
        parser.skipChildren();
    }

    @Override
    public Token currentToken() {
        return parser.currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return parser.currentName();
    }

    @Override
    public Map<String, Object> map() throws IOException {
        return parser.map();
    }

    @Override
    public Map<String, Object> mapOrdered() throws IOException {
        return parser.mapOrdered();
    }

    @Override
    public Map<String, String> mapStrings() throws IOException {
        return parser.mapStrings();
    }

    @Override
    public <T> Map<String, T> map(
            Supplier<Map<String, T>> mapFactory, CheckedFunction<XContentParser, T, IOException> mapValueParser) throws IOException {
        return parser.map(mapFactory, mapValueParser);
    }

    @Override
    public List<Object> list() throws IOException {
        return parser.list();
    }

    @Override
    public List<Object> listOrderedMap() throws IOException {
        return parser.listOrderedMap();
    }

    @Override
    public String text() throws IOException {
        return parser.text();
    }

    @Override
    public String textOrNull() throws IOException {
        return parser.textOrNull();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        return parser.charBufferOrNull();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return parser.charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return parser.objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return parser.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return parser.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return parser.textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return parser.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return parser.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return parser.numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return parser.numberType();
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        return parser.shortValue(coerce);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        return parser.intValue(coerce);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return parser.longValue(coerce);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        return parser.floatValue(coerce);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        return parser.doubleValue(coerce);
    }

    @Override
    public short shortValue() throws IOException {
        return parser.shortValue();
    }

    @Override
    public int intValue() throws IOException {
        return parser.intValue();
    }

    @Override
    public long longValue() throws IOException {
        return parser.longValue();
    }

    @Override
    public float floatValue() throws IOException {
        return parser.floatValue();
    }

    @Override
    public double doubleValue() throws IOException {
        return parser.doubleValue();
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        return parser.isBooleanValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        return parser.booleanValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return parser.binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return parser.getTokenLocation();
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return parser.namedObject(categoryClass, name, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return parser.getXContentRegistry();
    }

    @Override
    public boolean isClosed() {
        return parser.isClosed();
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return parser.getDeprecationHandler();
    }
}
