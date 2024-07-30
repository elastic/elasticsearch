/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.support.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.FilterXContentParserWrapper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.common.secret.Secret;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;

import java.io.IOException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;

/**
 * A xcontent parser that is used by watcher. This is a special parser that is
 * aware of watcher services. In particular, it's aware of the used {@link Clock}
 * and the CryptoService. The former (clock) may be used when the current time
 * is required during the parse phase of construct. The latter (crypto service) is used
 * to encode secret values (e.g. passwords, security tokens, etc..) to {@link Secret}s.
 * {@link Secret}s are encrypted values that are stored in memory and are decrypted
 * on demand when needed.
 */
public class WatcherXContentParser extends FilterXContentParserWrapper {

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

        if (parser instanceof WatcherXContentParser watcherParser) {
            if (REDACTED_PASSWORD.equals(text)) {
                if (watcherParser.allowRedactedPasswords) {
                    return null;
                } else {
                    throw new ElasticsearchParseException("found redacted password in field [{}]", parser.currentName());
                }
            } else if (watcherParser.cryptoService != null) {
                char[] encryptedChars = watcherParser.cryptoService.encrypt(chars);
                Arrays.fill(chars, '\0'); // Clear chars from unencrypted buffer
                return new Secret(encryptedChars);
            }
        }

        return new Secret(chars);
    }

    private final ZonedDateTime parseTime;
    @Nullable
    private final CryptoService cryptoService;
    private final boolean allowRedactedPasswords;

    public WatcherXContentParser(
        XContentParser parser,
        ZonedDateTime parseTime,
        @Nullable CryptoService cryptoService,
        boolean allowRedactedPasswords
    ) {
        super(parser);
        this.parseTime = parseTime;
        this.cryptoService = cryptoService;
        this.allowRedactedPasswords = allowRedactedPasswords;
    }

    public ZonedDateTime getParseDateTime() {
        return parseTime;
    }
}
