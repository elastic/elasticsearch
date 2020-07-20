/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.action.BasicFormatter.FormatOption.TEXT;

/**
 * Templating class for displaying SQL responses in text formats.
 */
enum TextFormat {

    /**
     * Default text writer.
     *
     * The implementation is a bit weird since state needs to be passed around, namely the formatter
     * since it is initialized based on the first page of data.
     * To avoid leaking the formatter, it gets discovered again in the wrapping method to attach it
     * to the next cursor and so on.
     */
    PLAIN_TEXT() {
        @Override
        String format(RestRequest request, SqlQueryResponse response) {
            BasicFormatter formatter = null;
            Cursor cursor = null;
            ZoneId zoneId = null;

            // check if the cursor is already wrapped first
            if (response.hasCursor()) {
                Tuple<Cursor, ZoneId> tuple = Cursors.decodeFromStringWithZone(response.cursor());
                cursor = tuple.v1();
                zoneId = tuple.v2();
                if (cursor instanceof TextFormatterCursor) {
                    formatter = ((TextFormatterCursor) cursor).getFormatter();
                }
            }

            // if there are headers available, it means it's the first request
            // so initialize the underlying formatter and wrap it in the cursor
            if (response.columns() != null) {
                formatter = new BasicFormatter(response.columns(), response.rows(), TEXT);
                // if there's a cursor, wrap the formatter in it
                if (cursor != null) {
                    response.cursor(Cursors.encodeToString(new TextFormatterCursor(cursor, formatter), zoneId));
                }
                // format with header
                return formatter.formatWithHeader(response.columns(), response.rows());
            }
            else {
                // should be initialized (wrapped by the cursor)
                if (formatter != null) {
                    // format without header
                    return formatter.formatWithoutHeader(response.rows());
                }
            }
            // if this code is reached, it means it's a next page without cursor wrapping
            throw new SqlIllegalArgumentException("Cannot find text formatter - this is likely a bug");
        }

        @Override
        String shortName() {
            return "txt";
        }

        @Override
        String contentType() {
            return "text/plain";
        }

        @Override
        protected Character delimiter() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String eol() {
            throw new UnsupportedOperationException();
        }
    },

    /**
     * Comma Separated Values implementation.
     *
     * Based on:
     * https://tools.ietf.org/html/rfc4180
     * https://www.iana.org/assignments/media-types/text/csv
     * https://www.w3.org/TR/sparql11-results-csv-tsv/
     *
     */
    CSV() {
        private Character delimiter = ',';

        @Override
        protected Character delimiter() {
            return delimiter;
        }

        @Override
        protected void setDelimiter(Character delimiter) {
            if (delimiter == null) {
                throw new UnsupportedOperationException();
            }
            switch (delimiter) {
                case '"':
                case '\n':
                case '\r':
                    throw new IllegalArgumentException("illegal reserved character specified as delimiter [" + delimiter + "]");
                case '\t':
                    throw new IllegalArgumentException("illegal delimiter [TAB] specified as delimiter for the [CSV] format; " +
                        "choose the [TSV] format instead");
            }
            this.delimiter = delimiter;
        }

        @Override
        protected String eol() {
            //CRLF
            return "\r\n";
        }

        @Override
        String shortName() {
            return "csv";
        }

        @Override
        String contentType() {
            return "text/csv";
        }

        @Override
        String contentType(RestRequest request) {
            return contentType() + "; charset=utf-8; header=" + (needsHeader(request) ? "present" : "absent");
        }

        @Override
        String maybeEscape(String value) {
            boolean needsEscaping = false;

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"' || c == '\n' || c == '\r' || c == delimiter()) {
                    needsEscaping = true;
                    break;
                }
            }

            if (needsEscaping) {
                StringBuilder sb = new StringBuilder();

                sb.append('"');
                for (int i = 0; i < value.length(); i++) {
                    char c = value.charAt(i);
                    if (value.charAt(i) == '"') {
                        sb.append('"');
                    }
                    sb.append(c);
                }
                sb.append('"');
                value = sb.toString();
            }

            return value;
        }

        @Override
        boolean needsHeader(RestRequest request) {
            String header = request.param("header");
            if (header == null) {
                List<String> values = request.getAllHeaderValues("Accept");
                if (values != null) {
                    // header values are separated by `;` so try breaking it down
                    for (String value : values) {
                        String[] params = Strings.tokenizeToStringArray(value, ";");
                        for (String param : params) {
                            if (param.toLowerCase(Locale.ROOT).equals("header=absent")) {
                                return false;
                            }
                        }
                    }
                }
                return true;
            } else {
                return !header.toLowerCase(Locale.ROOT).equals("absent");
            }
        }
    },

    TSV() {
        @Override
        protected Character delimiter() {
            return '\t';
        }

        @Override
        protected String eol() {
            // only LF
            return "\n";
        }

        @Override
        String shortName() {
            return "tsv";
        }

        @Override
        String contentType() {
            return "text/tab-separated-values";
        }

        @Override
        String contentType(RestRequest request) {
            return contentType() + "; charset=utf-8";
        }

        @Override
        String maybeEscape(String value) {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '\n' :
                        sb.append("\\n");
                        break;
                    case '\t' :
                        sb.append("\\t");
                        break;
                    case '\\' :
                        sb.append("\\\\");
                        break;
                    default:
                        sb.append(c);
                }
            }

            return sb.toString();
        }
    };


    String format(RestRequest request, SqlQueryResponse response) {
        if (this == CSV) {
            String delimiter = request.param("delimiter");
            if (delimiter != null) {
                if (delimiter.length() != 1) {
                    throw new IllegalArgumentException("invalid " +
                        (delimiter.length() > 0 ? "multi-character" : "empty") + " delimiter [" + delimiter + "]");
                }
                this.setDelimiter(delimiter.charAt(0));
            }
        }

        StringBuilder sb = new StringBuilder();

        // if the header is requested (and the column info is present - namely it's the first page) return the info
        if (needsHeader(request) && response.columns() != null) {
            row(sb, response.columns(), ColumnInfo::name);
        }

        for (List<Object> row : response.rows()) {
            row(sb, row, f -> f instanceof ZonedDateTime ? DateUtils.toString((ZonedDateTime) f) : Objects.toString(f, StringUtils.EMPTY));
        }

        return sb.toString();
    }

    boolean needsHeader(RestRequest request) {
        return true;
    }

    static TextFormat fromMediaTypeOrFormat(String accept) {
        for (TextFormat text : values()) {
            String contentType = text.contentType();
            if (contentType.equalsIgnoreCase(accept)
                    || accept.toLowerCase(Locale.ROOT).startsWith(contentType + ";")
                    || text.shortName().equalsIgnoreCase(accept)) {
                return text;
            }
        }

        throw new IllegalArgumentException("invalid format [" + accept + "]");
    }

    /**
     * Short name typically used by format parameter.
     * Can differ from the IANA mime type.
     */
    abstract String shortName();


    /**
     * Formal IANA mime type.
     */
    abstract String contentType();

    /**
     * Content type depending on the request.
     * Might be used by some formatters (like CSV) to specify certain metadata like
     * whether the header is returned or not.
     */
    String contentType(RestRequest request) {
        return contentType();
    }

    // utility method for consuming a row.
    <F> void row(StringBuilder sb, List<F> row, Function<F, String> toString) {
        for (int i = 0; i < row.size(); i++) {
            sb.append(maybeEscape(toString.apply(row.get(i))));
            if (i < row.size() - 1) {
                sb.append(delimiter());
            }
        }
        sb.append(eol());
    }

    /**
     * Delimiter between fields
     */
    protected abstract Character delimiter();

    protected void setDelimiter(Character delimiter) {
        throw new UnsupportedOperationException();
    }

    /**
     * String indicating end-of-line or row.
     */
    protected abstract String eol();

    /**
     * Method used for escaping (if needed) a given value.
     */
    String maybeEscape(String value) {
        return value;
    }
}
