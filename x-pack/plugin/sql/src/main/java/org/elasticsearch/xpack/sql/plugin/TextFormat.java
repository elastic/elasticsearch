/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.BasicFormatter;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.proto.Protocol.URL_PARAM_DELIMITER;
import static org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter.FormatOption.TEXT;

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
            } else if (formatter != null) { // should be initialized (wrapped by the cursor)
                // format without header
                return formatter.formatWithoutHeader(response.rows());
            } else if (response.hasId()) {
                // an async request has no results yet
                return StringUtils.EMPTY;
            } else if (response.rows().isEmpty()) {
                // no data and no headers to return
                return StringUtils.EMPTY;
            }
            // if this code is reached, it means it's a next page without cursor wrapping
            throw new SqlIllegalArgumentException("Cannot find text formatter - this is likely a bug");
        }

        @Override
        String shortName() {
            return FORMAT_TEXT;
        }

        @Override
        String contentType() {
            return CONTENT_TYPE_TXT;
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
        @Override
        protected Character delimiter() {
            return ',';
        }

        @Override
        protected String eol() {
            // CRLF
            return "\r\n";
        }

        @Override
        String shortName() {
            return FORMAT_CSV;
        }

        @Override
        String contentType() {
            return CONTENT_TYPE_CSV;
        }

        @Override
        String contentType(RestRequest request) {
            return contentType()
                + "; charset=utf-8; "
                + URL_PARAM_HEADER
                + "="
                + (hasHeader(request) ? PARAM_HEADER_PRESENT : PARAM_HEADER_ABSENT);
        }

        @Override
        protected Character delimiter(RestRequest request) {
            String delimiterParam = request.param(URL_PARAM_DELIMITER);
            if (delimiterParam == null) {
                return delimiter();
            }
            try {
                delimiterParam = URLDecoder.decode(delimiterParam, StandardCharsets.UTF_8.toString());
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("delimiter [" + delimiterParam + "] cannot be decoded: " + uee.getMessage(), uee);
            }
            if (delimiterParam.length() != 1) {
                throw new IllegalArgumentException(
                    "invalid " + (delimiterParam.length() > 0 ? "multi-character" : "empty") + " delimiter [" + delimiterParam + "]"
                );
            }
            Character delimiter = delimiterParam.charAt(0);
            switch (delimiter) {
                case '"':
                case '\n':
                case '\r':
                    throw new IllegalArgumentException("illegal reserved character specified as delimiter [" + delimiter + "]");
                case '\t':
                    throw new IllegalArgumentException(
                        "illegal delimiter [TAB] specified as delimiter for the [csv] format; " + "choose the [tsv] format instead"
                    );
            }
            return delimiter;
        }

        @Override
        String maybeEscape(String value, Character delimiter) {
            boolean needsEscaping = false;

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"' || c == '\n' || c == '\r' || c == delimiter) {
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
        boolean hasHeader(RestRequest request) {
            String header = request.param(URL_PARAM_HEADER);
            if (header == null) {
                List<String> values = request.getAllHeaderValues("Accept");
                if (values != null) {
                    // header values are separated by `;` so try breaking it down
                    for (String value : values) {
                        String[] params = Strings.tokenizeToStringArray(value, ";");
                        for (String param : params) {
                            if (param.toLowerCase(Locale.ROOT).equals(URL_PARAM_HEADER + "=" + PARAM_HEADER_ABSENT)) {
                                return false;
                            }
                        }
                    }
                }
                return true;
            } else {
                return header.toLowerCase(Locale.ROOT).equals(PARAM_HEADER_ABSENT) == false;
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
            return FORMAT_TSV;
        }

        @Override
        String contentType() {
            return CONTENT_TYPE_TSV;
        }

        @Override
        String contentType(RestRequest request) {
            return contentType() + "; charset=utf-8";
        }

        @Override
        String maybeEscape(String value, Character __) {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    default:
                        sb.append(c);
                }
            }

            return sb.toString();
        }
    };

    private static final String FORMAT_TEXT = "txt";
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_TSV = "tsv";
    private static final String CONTENT_TYPE_TXT = "text/plain";
    private static final String CONTENT_TYPE_CSV = "text/csv";
    private static final String CONTENT_TYPE_TSV = "text/tab-separated-values";
    private static final String URL_PARAM_HEADER = "header";
    private static final String PARAM_HEADER_ABSENT = "absent";
    private static final String PARAM_HEADER_PRESENT = "present";

    String format(RestRequest request, SqlQueryResponse response) {
        StringBuilder sb = new StringBuilder();

        // if the header is requested (and the column info is present - namely it's the first page) return the info
        if (hasHeader(request) && response.columns() != null) {
            row(sb, response.columns(), ColumnInfo::name, delimiter(request));
        }

        for (List<Object> row : response.rows()) {
            row(
                sb,
                row,
                f -> f instanceof ZonedDateTime ? DateUtils.toString((ZonedDateTime) f) : Objects.toString(f, StringUtils.EMPTY),
                delimiter(request)
            );
        }

        return sb.toString();
    }

    boolean hasHeader(RestRequest request) {
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
    <F> void row(StringBuilder sb, List<F> row, Function<F, String> toString, Character delimiter) {
        for (int i = 0; i < row.size(); i++) {
            sb.append(maybeEscape(toString.apply(row.get(i)), delimiter));
            if (i < row.size() - 1) {
                sb.append(delimiter);
            }
        }
        sb.append(eol());
    }

    /**
     * Delimiter between fields
     */
    protected abstract Character delimiter();

    protected Character delimiter(RestRequest request) {
        return delimiter();
    }

    /**
     * String indicating end-of-line or row.
     */
    protected abstract String eol();

    /**
     * Method used for escaping (if needed) a given value.
     */
    String maybeEscape(String value, Character delimiter) {
        return value;
    }
}
