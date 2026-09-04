/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class RegexRecordSplitter implements RecordSplitter {

    private final Pattern terminatorPattern;
    private final int maxRecordBytes;

    RegexRecordSplitter(String terminatorRegex, int maxRecordBytes) {
        if (maxRecordBytes <= 0) {
            throw new IllegalArgumentException("maxRecordBytes must be positive");
        }
        this.terminatorPattern = Pattern.compile(Objects.requireNonNull(terminatorRegex));
        this.maxRecordBytes = maxRecordBytes;
    }

    @Override
    public long findNextRecordBoundary(InputStream stream) throws IOException {
        StringBuilder consumedBytes = new StringBuilder();
        int next;
        while ((next = stream.read()) != -1) {
            consumedBytes.append((char) (next & 0xFF));
            if (consumedBytes.length() > maxRecordBytes) {
                return RECORD_TOO_LARGE;
            }
            if (terminates(consumedBytes)) {
                return consumedBytes.length();
            }
        }
        return -1;
    }

    @Override
    public int findLastRecordBoundary(byte[] buf, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, buf.length);
        String chunk = new String(buf, offset, length, StandardCharsets.ISO_8859_1);
        Matcher matcher = terminatorPattern.matcher(chunk);
        int lastBoundary = -1;
        while (matcher.find()) {
            if (matcher.start() == matcher.end()) {
                throw new IllegalArgumentException("terminatorRegex must not match an empty sequence");
            }
            lastBoundary = offset + matcher.end() - 1;
        }
        return lastBoundary;
    }

    @Override
    public int maxRecordBytes() {
        return maxRecordBytes;
    }

    private boolean terminates(CharSequence consumedBytes) {
        Matcher matcher = terminatorPattern.matcher(consumedBytes);
        while (matcher.find()) {
            if (matcher.start() == matcher.end()) {
                throw new IllegalArgumentException("terminatorRegex must not match an empty sequence");
            }
            if (matcher.end() == consumedBytes.length()) {
                return true;
            }
        }
        return false;
    }
}
