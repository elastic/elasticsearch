/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Templating class for displaying ESQL responses in text formats.
 */
public enum TextFormat implements MediaType {

    /**
     * Default text writer.
     */
    PLAIN_TEXT() {
        @Override
        public String format(RestRequest request, EsqlQueryResponse esqlResponse) {
            return new TextFormatter(esqlResponse).format(hasHeader(request));
        }

        @Override
        public String queryParameter() {
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

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(CONTENT_TYPE_TXT, Map.of("header", "present|absent")),
                new HeaderValue(
                    VENDOR_CONTENT_TYPE_TXT,
                    Map.of("header", "present|absent", COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
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
        public String queryParameter() {
            return FORMAT_CSV;
        }

        @Override
        String contentType() {
            return CONTENT_TYPE_CSV;
        }

        @Override
        public String contentType(RestRequest request) {
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
            delimiterParam = URLDecoder.decode(delimiterParam, StandardCharsets.UTF_8);
            if (delimiterParam.length() != 1) {
                throw new IllegalArgumentException(
                    "invalid " + (delimiterParam.length() > 0 ? "multi-character" : "empty") + " delimiter [" + delimiterParam + "]"
                );
            }
            Character delimiter = delimiterParam.charAt(0);
            switch (delimiter) {
                case '"', '\n', '\r' -> throw new IllegalArgumentException(
                    "illegal reserved character specified as delimiter [" + delimiter + "]"
                );
                case '\t' -> throw new IllegalArgumentException(
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

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(CONTENT_TYPE_CSV, Map.of("header", "present|absent", "delimiter", ".+")),// more detailed parsing is in
                                                                                                         // TextFormat.CSV#delimiter
                new HeaderValue(
                    VENDOR_CONTENT_TYPE_CSV,
                    Map.of("header", "present|absent", "delimiter", ".+", COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
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
        public String queryParameter() {
            return FORMAT_TSV;
        }

        @Override
        String contentType() {
            return CONTENT_TYPE_TSV;
        }

        @Override
        public String contentType(RestRequest request) {
            return contentType() + "; charset=utf-8";
        }

        @Override
        String maybeEscape(String value, Character __) {
            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                switch (c) {
                    case '\n' -> sb.append("\\n");
                    case '\t' -> sb.append("\\t");
                    default -> sb.append(c);
                }
            }

            return sb.toString();
        }

        @Override
        public Set<HeaderValue> headerValues() {
            return Set.of(
                new HeaderValue(CONTENT_TYPE_TSV, Map.of("header", "present|absent")),
                new HeaderValue(
                    VENDOR_CONTENT_TYPE_TSV,
                    Map.of("header", "present|absent", COMPATIBLE_WITH_PARAMETER_NAME, VERSION_PATTERN)
                )
            );
        }
    };

    private static final String FORMAT_TEXT = "txt";
    private static final String FORMAT_CSV = "csv";
    private static final String FORMAT_TSV = "tsv";
    private static final String CONTENT_TYPE_TXT = "text/plain";
    private static final String VENDOR_CONTENT_TYPE_TXT = "text/vnd.elasticsearch+plain";
    private static final String CONTENT_TYPE_CSV = "text/csv";
    private static final String VENDOR_CONTENT_TYPE_CSV = "text/vnd.elasticsearch+csv";
    private static final String CONTENT_TYPE_TSV = "text/tab-separated-values";
    private static final String VENDOR_CONTENT_TYPE_TSV = "text/vnd.elasticsearch+tab-separated-values";
    private static final String URL_PARAM_HEADER = "header";
    private static final String PARAM_HEADER_ABSENT = "absent";
    private static final String PARAM_HEADER_PRESENT = "present";
    /*
     * URL parameters
     */
    public static final String URL_PARAM_FORMAT = "format";
    public static final String URL_PARAM_DELIMITER = "delimiter";

    public String format(RestRequest request, EsqlQueryResponse esqlResponse) {
        StringBuilder sb = new StringBuilder();

        // if the header is requested return the info
        if (hasHeader(request) && esqlResponse.columns() != null) {
            row(sb, esqlResponse.columns(), ColumnInfo::name, delimiter(request));
        }

        for (List<Object> row : esqlResponse.values()) {
            row(sb, row, f -> Objects.toString(f, StringUtils.EMPTY), delimiter(request));
        }

        return sb.toString();
    }

    boolean hasHeader(RestRequest request) {
        return true;
    }

    /**
     * Formal IANA mime type.
     */
    abstract String contentType();

    /**
     * Content type depending on the request.
     * Might be used by some formatters (like CSV) to specify certain metadata like
     * whether the header is returned or not.
     */
    public String contentType(RestRequest request) {
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
