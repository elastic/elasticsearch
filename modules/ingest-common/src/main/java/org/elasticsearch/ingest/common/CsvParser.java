/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;

final class CsvParser {

    private static final char LF = '\n';
    private static final char CR = '\r';
    private static final char SPACE = ' ';
    private static final char TAB = '\t';

    private enum State {
        START, UNQUOTED, QUOTED, QUOTED_END
    }

    private final char quote;
    private final char separator;
    private final boolean trim;
    private final String[] headers;
    private final Object emptyValue;
    private final IngestDocument ingestDocument;
    private final StringBuilder builder = new StringBuilder();
    private State state = State.START;
    private String line;
    private int currentHeader = 0;
    private int startIndex = 0;
    private int length;
    private int currentIndex;

    CsvParser(IngestDocument ingestDocument, char quote, char separator, boolean trim, String[] headers, Object emptyValue) {
        this.ingestDocument = ingestDocument;
        this.quote = quote;
        this.separator = separator;
        this.trim = trim;
        this.headers = headers;
        this.emptyValue = emptyValue;
    }

    void process(String line) {
        this.line = line;
        length = line.length();
        for (currentIndex = 0; currentIndex < length; currentIndex++) {
            switch (state) {
                case START:
                    if (processStart()) {
                        return;
                    }
                    break;
                case UNQUOTED:
                    if (processUnquoted()) {
                        return;
                    }
                    break;
                case QUOTED:
                    processQuoted();
                    break;
                case QUOTED_END:
                    if (processQuotedEnd()) {
                        return;
                    }
                    break;
            }
        }

        //we've reached end of string, we need to handle last field
        switch (state) {
            case UNQUOTED:
                setField(length);
                break;
            case QUOTED_END:
                setField(length - 1);
                break;
            case QUOTED:
                throw new IllegalArgumentException("Unmatched quote");
        }
    }

    private boolean processStart() {
        for (; currentIndex < length; currentIndex++) {
            char c = currentChar();
            if (c == quote) {
                state = State.QUOTED;
                builder.setLength(0);
                startIndex = currentIndex + 1;
                return false;
            } else if (c == separator) {
                startIndex++;
                builder.setLength(0);
                if (setField(startIndex)) {
                    return true;
                }
            } else if (isWhitespace(c)) {
                if (trim) {
                    startIndex++;
                }
            } else {
                state = State.UNQUOTED;
                builder.setLength(0);
                return false;
            }
        }
        return true;
    }

    private boolean processUnquoted() {
        int spaceCount = 0;
        for (; currentIndex < length; currentIndex++) {
            char c = currentChar();
            if (c == LF || c == CR || c == quote) {
                throw new IllegalArgumentException("Illegal character inside unquoted field at " + currentIndex);
            } else if (trim && isWhitespace(c)) {
                spaceCount++;
            } else if (c == separator) {
                state = State.START;
                if (setField(currentIndex - spaceCount)) {
                    return true;
                }
                startIndex = currentIndex + 1;
                return false;
            } else {
                spaceCount = 0;
            }
        }
        return false;
    }

    private void processQuoted() {
        for (; currentIndex < length; currentIndex++) {
            if (currentChar() == quote) {
                state = State.QUOTED_END;
                break;
            }
        }
    }

    private boolean processQuotedEnd() {
        char c = currentChar();
        if (c == quote) {
            builder.append(line, startIndex, currentIndex - 1).append(quote);
            startIndex = currentIndex + 1;
            state = State.QUOTED;
            return false;
        }
        boolean shouldSetField = true;
        for (; currentIndex < length; currentIndex++) {
            c = currentChar();
            if (isWhitespace(c)) {
                if (shouldSetField) {
                    if (setField(currentIndex - 1)) {
                        return true;
                    }
                    shouldSetField = false;
                }
            } else if (c == separator) {
                if (shouldSetField && setField(currentIndex - 1)) {
                    return true;
                }
                startIndex = currentIndex + 1;
                state = State.START;
                return false;
            } else {
                throw new IllegalArgumentException("character '" + c + "' after quoted field at " + currentIndex);
            }
        }
        return true;
    }

    private char currentChar() {
        return line.charAt(currentIndex);
    }

    private boolean isWhitespace(char c) {
        return c == SPACE || c == TAB;
    }

    private boolean setField(int endIndex) {
        String value;
        if (builder.length() == 0) {
            value = line.substring(startIndex, endIndex);
        } else {
            value = builder.append(line, startIndex, endIndex).toString();
        }
        if (value.length() > 0) {
            ingestDocument.setFieldValue(headers[currentHeader], value);
        } else if (emptyValue != null) {
            ingestDocument.setFieldValue(headers[currentHeader], emptyValue);
        }
        currentHeader++;
        return currentHeader == headers.length;
    }
}
