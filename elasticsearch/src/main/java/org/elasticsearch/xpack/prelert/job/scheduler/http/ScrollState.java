/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.scheduler.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Holds the state of an Elasticsearch scroll.
 */
class ScrollState {

    private static final Pattern SCROLL_ID_PATTERN = Pattern.compile("\"_scroll_id\":\"(.*?)\"");
    private static final Pattern DEFAULT_PEEK_END_PATTERN = Pattern.compile("\"hits\":\\[(.)");
    private static final Pattern AGGREGATED_PEEK_END_PATTERN = Pattern.compile("\"aggregations\":.*?\"buckets\":\\[(.)");
    private static final String CLOSING_SQUARE_BRACKET = "]";

    /**
     * We want to read up until the "hits" or "buckets" array. Scroll IDs can be
     * quite long in clusters that have many nodes/shards. The longest reported
     * scroll ID is 20708 characters - see
     * http://elasticsearch-users.115913.n3.nabble.com/Ridiculously-long-Scroll-id-td4038567.html
     * <br>
     * We set a max byte limit for the stream peeking to 1 MB.
     */
    private static final int MAX_PEEK_BYTES = 1024 * 1024;

    /**
     * We try to search for the scroll ID and whether the scroll is complete every time
     * we have read a chunk of size 32 KB.
     */
    private static final int PEEK_CHUNK_SIZE = 32 * 1024;

    private final int peekMaxSize;
    private final int peekChunkSize;
    private final Pattern peekEndPattern;
    private volatile String scrollId;
    private volatile boolean isComplete;

    private ScrollState(int peekMaxSize, int peekChunkSize, Pattern scrollCompletePattern) {
        this.peekMaxSize = peekMaxSize;
        this.peekChunkSize = peekChunkSize;
        peekEndPattern = Objects.requireNonNull(scrollCompletePattern);
    }

    /**
     * Creates a {@code ScrollState} for a search without aggregations
     * @return the {@code ScrollState}
     */
    public static ScrollState createDefault() {
        return new ScrollState(MAX_PEEK_BYTES, PEEK_CHUNK_SIZE, DEFAULT_PEEK_END_PATTERN);
    }

    /**
     * Creates a {@code ScrollState} for a search with aggregations
     * @return the {@code ScrollState}
     */
    public static ScrollState createAggregated() {
        return new ScrollState(MAX_PEEK_BYTES, PEEK_CHUNK_SIZE, AGGREGATED_PEEK_END_PATTERN);
    }

    public final void reset() {
        scrollId = null;
        isComplete = false;
    }

    public void clearScrollId() {
        scrollId = null;
    }

    public String getScrollId() {
        return scrollId;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void forceComplete() {
        isComplete = true;
    }

    /**
     * Peeks into the stream and updates the scroll ID and whether the scroll is complete.
     * <p>
     * <em>After calling that method the given stream cannot be reused.
     * Use the returned stream instead.</em>
     * </p>
     *
     * @param stream the stream
     * @return a new {@code InputStream} object which should be used instead of the given stream
     * for further processing
     * @throws IOException if an I/O error occurs while manipulating the stream or the stream
     * contains no scroll ID
     */
    public InputStream updateFromStream(InputStream stream) throws IOException {
        if (stream == null) {
            isComplete = true;
            return null;
        }

        PushbackInputStream pushbackStream = new PushbackInputStream(stream, peekMaxSize);
        byte[] buffer = new byte[peekMaxSize];
        int totalBytesRead = 0;
        int currentChunkSize = 0;

        int bytesRead = 0;
        while (bytesRead >= 0 && totalBytesRead < peekMaxSize) {
            bytesRead = stream.read(buffer, totalBytesRead, Math.min(peekChunkSize, peekMaxSize - totalBytesRead));
            if (bytesRead > 0) {
                totalBytesRead += bytesRead;
                currentChunkSize += bytesRead;
            }

            if (bytesRead < 0 || currentChunkSize >= peekChunkSize) {
                // We make the assumption here that invalid byte sequences will be read as invalid
                // char rather than throwing an exception
                String peekString = new String(buffer, 0, totalBytesRead, StandardCharsets.UTF_8);

                if (matchScrollState(peekString)) {
                    break;
                }
                currentChunkSize = 0;
            }
        }

        pushbackStream.unread(buffer, 0, totalBytesRead);

        if (scrollId == null) {
            throw new IOException("Field '_scroll_id' was expected but not found in first "
                    + totalBytesRead + " bytes of response:\n"
                    + HttpResponse.getStreamAsString(pushbackStream));
        }
        return pushbackStream;
    }

    /**
     * Searches the peek end pattern into the given {@code sequence}. If it is matched,
     * it also searches for the scroll ID and updates the state accordingly.
     *
     * @param sequence the String to search into
     * @return {@code true} if the peek end pattern was matched or {@code false} otherwise
     */
    private boolean matchScrollState(String sequence) {
        Matcher peekEndMatcher = peekEndPattern.matcher(sequence);
        if (peekEndMatcher.find()) {
            Matcher scrollIdMatcher = SCROLL_ID_PATTERN.matcher(sequence);
            if (!scrollIdMatcher.find()) {
                scrollId = null;
            } else {
                scrollId = scrollIdMatcher.group(1);
                isComplete = CLOSING_SQUARE_BRACKET.equals(peekEndMatcher.group(1));
                return true;
            }
        }
        return false;
    }
}
