/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class UriUtils {
    private UriUtils() {

    }

    static final String HTTP_SCHEME = "http";
    static final String HTTPS_SCHEME = "https";
    static final String HTTP_PREFIX = HTTP_SCHEME + "://";
    static final String HTTPS_PREFIX = HTTPS_SCHEME + "://";

    /**
     * Parses the URL provided by the user and substitutes any missing parts in it with defaults read from the defaultURI.
     * The result is returned as an URI object.
     * In case of a parsing exception, the credentials are redacted from the URISyntaxException message.
     */
    public static URI parseURI(String connectionString, URI defaultURI) {
        final URI uri = parseMaybeWithScheme(connectionString, defaultURI.getScheme() + "://");
        // Repack the connection string with provided default elements - where missing from the original string - and reparse into a URI.
        final String scheme = uri.getScheme() != null ? uri.getScheme() : defaultURI.getScheme();
        // TODO: support for IDN
        final String host = uri.getHost() != null ? uri.getHost() : defaultURI.getHost();
        final String path = "".equals(uri.getPath()) ? defaultURI.getPath() : uri.getPath();
        final String rawQuery = uri.getQuery() == null ? defaultURI.getRawQuery() : uri.getRawQuery();
        final String rawFragment = uri.getFragment() == null ? defaultURI.getRawFragment() : uri.getRawFragment();
        final int port = uri.getPort() < 0 ? defaultURI.getPort() : uri.getPort();
        try {
            // The query part is attached in original "raw" format, to preserve the escaping of characters. This is needed since any
            // escaped query structure characters (`&` and `=`) wouldn't remain escaped when passed back through the URI constructor
            // (since they are legal in the query part), and that would later interfere with correctly parsing the attributes.
            // And same with escaped `#` chars in the fragment part.
            String connStr = new URI(scheme, uri.getUserInfo(), host, port, path, null, null).toString();
            if (StringUtils.hasLength(rawQuery)) {
                connStr += "?" + rawQuery;
            }
            if (StringUtils.hasLength(rawFragment)) {
                connStr += "#" + rawFragment;
            }
            return new URI(connStr);
        } catch (URISyntaxException e) {
            // should only happen if the defaultURI is malformed
            throw new IllegalArgumentException("Invalid connection configuration [" + connectionString + "]: " + e.getMessage(), e);
        }
    }

    private static URI parseMaybeWithScheme(String connectionString, String defaultPrefix) {
        URI uri;
        String c = connectionString.toLowerCase(Locale.ROOT);
        boolean hasAnHttpPrefix = c.startsWith(HTTP_PREFIX) || c.startsWith(HTTPS_PREFIX);
        try {
            uri = new URI(connectionString);
        } catch (URISyntaxException e) {
            // If the connection string contains no scheme plus an IP address with semicolon - like an IPv6 ([::1]), or an IPv4 plus port
            // (127.0.0.1:9200) - the URI parser will fail, as it'll try to interpret the pre-`:` chars as a scheme.
            if (hasAnHttpPrefix == false) {
                return parseMaybeWithScheme(defaultPrefix + connectionString, null);
            }
            URISyntaxException s = CredentialsRedaction.redactedURISyntaxException(e);
            throw new IllegalArgumentException("Invalid connection configuration: " + s.getMessage(), s);
        }

        if (hasAnHttpPrefix == false) {
            if (uri.getHost() != null) { // URI is valid and with a host, so there's a scheme (otherwise host==null), but just not HTTP(S)
                throw new IllegalArgumentException(
                    "Invalid connection scheme [" + uri.getScheme() + "] configuration: only " + HTTP_SCHEME + " and " + HTTPS_SCHEME
                        + " protocols are supported");
            }
            // no host and either (1) no scheme (like for input 'host') or (2) invalid scheme (produced by parsing 'user:pass@host' or
            // 'host:9200' or just erroneous: 'ftp:/?foo' etc.): try with a HTTP scheme
            if (connectionString.length() > 0) { // an empty string is a valid connection string
                return parseMaybeWithScheme(defaultPrefix + connectionString, null);
            }
        }

        return uri;
    }

    public static class CredentialsRedaction {
        public static final Character REDACTION_CHAR = '*';
        private static final String USER_ATTR_NAME = "user";
        private static final String PASS_ATTR_NAME = "password";

        // redacts the value of a named attribute in a given string, by finding the substring `attrName=`; everything following that is
        // considered as attribute's value.
        private static String redactAttributeInString(String string, String attrName, Character replacement) {
            String needle = attrName + "=";
            int attrIdx = string.toLowerCase(Locale.ROOT).indexOf(needle); // note: won't catch "valid" `=password[%20]+=` cases
            if (attrIdx >= 0) { // ex: `...=[value]password=foo...`
                int attrEndIdx = attrIdx + needle.length();
                return string.substring(0, attrEndIdx) + String.valueOf(replacement).repeat(string.length() - attrEndIdx);
            }
            return string;
        }

        private static void redactValueForSimilarKey(String key, List<String> options, List<Map.Entry<String, String>> attrs,
                                                     Character replacement) {
            List<String> similar = StringUtils.findSimilar(key, options);
            for (String k : similar) {
                for (Map.Entry<String, String> e : attrs) {
                    if (e.getKey().equals(k)) {
                        e.setValue(String.valueOf(replacement).repeat(e.getValue().length()));
                    }
                }
            }
        }

        public static String redactCredentialsInRawUriQuery(String rawQuery, Character replacement) {
            List<Map.Entry<String, String>> attrs = new ArrayList<>();
            List<String> options = new ArrayList<>();

            // break down the query in (key, value) tuples, redacting any malformed attribute values
            String key, value;
            for (String param : StringUtils.tokenize(rawQuery, "&")) {
                int eqIdx = param.indexOf('=');
                if (eqIdx <= 0) { // malformed param: no, or leading `=`: record entire param string as key and empty string as value
                    value = eqIdx < 0 ? null : StringUtils.EMPTY;
                    key = redactAttributeInString(param, USER_ATTR_NAME, replacement);
                    key = redactAttributeInString(key, PASS_ATTR_NAME, replacement);
                } else {
                    key = param.substring(0, eqIdx);
                    value = param.substring(eqIdx + 1);
                    if (value.indexOf('=') >= 0) { // `...&user=FOOpassword=BAR&...`
                        value = redactAttributeInString(value, USER_ATTR_NAME, replacement);
                        value = redactAttributeInString(value, PASS_ATTR_NAME, replacement);
                    }
                    options.add(key);
                }
                attrs.add(new AbstractMap.SimpleEntry<>(key, value));
            }

            // redact the credential attributes, as well as any other attribute that is similar to them, i.e. mistyped
            redactValueForSimilarKey(USER_ATTR_NAME, options, attrs, replacement);
            redactValueForSimilarKey(PASS_ATTR_NAME, options, attrs, replacement);

            // re-construct the query
            StringBuilder sb = new StringBuilder(rawQuery.length());
            for (Map.Entry<String, String> a : attrs) {
                sb.append("&");
                sb.append(a.getKey());
                if (a.getValue() != null) {
                    sb.append("=");
                    sb.append(a.getValue());
                }
            }
            return sb.substring(1);
        }

        private static String editURI(URI uri, List<Map.Entry<Integer, Character>> faults, boolean hasPort) {
            StringBuilder sb = new StringBuilder();
            if (uri.getScheme() != null) {
                sb.append(uri.getScheme());
                sb.append("://");
            }
            if (uri.getRawUserInfo() != null) {
                sb.append("\0".repeat(uri.getRawUserInfo().length()));
                if (uri.getHost() != null) {
                    sb.append('@');
                }
            }
            if (uri.getHost() != null) {
                sb.append(uri.getHost());
            }
            if (hasPort || uri.getPort() > 0) {
                sb.append(':');
            }
            if (uri.getPort() > 0) {
                sb.append(uri.getPort());
            }
            if (uri.getRawPath() != null) {
                sb.append(uri.getRawPath());
            }
            if (uri.getQuery() != null) {
                sb.append('?');
                // redact with the null character; this will later allow safe reinsertion of any character removed from the URI to make
                // it parsable
                sb.append(redactCredentialsInRawUriQuery(uri.getRawQuery(), '\0'));
            }
            if (uri.getRawFragment() != null) {
                sb.append('#');
                sb.append(uri.getRawFragment());
            }

            // reinsert any removed character back into the URI: if the reinsertion should be made between null characters, replace its
            // value with a null character, since it's part of the credential value
            Collections.reverse(faults);
            for (Map.Entry<Integer, Character> e : faults) {
                int idx = e.getKey();
                if (idx >= sb.length()) {
                    sb.append(e.getValue());
                } else {
                    sb.insert(idx,
                        (sb.charAt(idx) == '\0' && (idx + 1 >= sb.length() || sb.charAt(idx + 1) == '\0')) ? '\0' : e.getValue());
                }
            }

            StringBuilder ret = new StringBuilder();
            sb.chars().forEach(x -> ret.append(x == '\0' ? REDACTION_CHAR : (char) x));

            return ret.toString();
        }

        private static String redactCredentialsInURLString(String urlString) {
            List<Map.Entry<Integer, Character>> faults = new ArrayList<>();

            boolean hasPort = false;
            for (StringBuilder sb = new StringBuilder(urlString); sb.length() > 0; ) {
                try {
                    // parse as URL; ex. `http://ho~st` parses as URI, but with unparsable authority
                    URI uri = new URI(sb.toString()).parseServerAuthority();
                    return editURI(uri, faults, hasPort);
                } catch (URISyntaxException use) {
                    int idx = use.getIndex();
                    if (idx < 0 || idx >= sb.length()) {
                        break; // not a faulty character-related error
                    }
                    if (use.getReason().equals("Illegal character in port number")) {
                        // if entire port part is broken (ex. `localhost:noDigit`), the trailing `:` will be lost in the resulting URI
                        hasPort = true;
                    }
                    faults.add(new AbstractMap.SimpleImmutableEntry<>(use.getIndex(), sb.charAt(idx)));
                    sb.deleteCharAt(idx);
                }
            }
            return null;
        }

        public static String redactCredentialsInConnectionString(String connectionString) {
            if (connectionString.startsWith(HTTP_PREFIX.toUpperCase(Locale.ROOT))
                || connectionString.startsWith(HTTPS_PREFIX.toUpperCase(Locale.ROOT))
                // too short to contain credentials
                || connectionString.length() < "_:_@_".length()
                // In "[a] hierarchical URI [...] characters :, /, ?, and # stand for themselves": don't attempt to redact a URI that
                // contains no credentials.
                || (connectionString.indexOf('@') < 0 && connectionString.indexOf('?') < 0)) {
                return connectionString;
            }

            String cs = connectionString.toLowerCase(Locale.ROOT);
            boolean prefixed = cs.startsWith(HTTP_PREFIX) || cs.startsWith(HTTPS_PREFIX);
            String redacted = redactCredentialsInURLString((prefixed ? StringUtils.EMPTY : HTTP_PREFIX) + connectionString);
            if (redacted == null) {
                return "<REDACTED> ; a capitalized scheme (HTTP|HTTPS) disables the redaction";
            }
            return prefixed ? redacted : redacted.substring(HTTP_PREFIX.length());
        }

        public static URISyntaxException redactedURISyntaxException(URISyntaxException e) {
            return new URISyntaxException(redactCredentialsInConnectionString(e.getInput()), e.getReason(), e.getIndex());
        }

    }

    /**
     * Removes the query part of the URI
     */
    public static URI removeQuery(URI uri, String connectionString, URI defaultURI) {
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), null, defaultURI.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid connection configuration [" + connectionString + "]: " + e.getMessage(), e);
        }
    }

    public static URI appendSegmentToPath(URI uri, String segment) {
        if (uri == null) {
            throw new IllegalArgumentException("URI must not be null");
        }
        if (segment == null || segment.isEmpty() || "/".equals(segment)) {
            return uri;
        }
        
        String path = uri.getPath();
        String concatenatedPath = "";
        String cleanSegment = segment.startsWith("/") ? segment.substring(1) : segment;
        
        if (path == null || path.isEmpty()) {
            path = "/";
        }

        if (path.charAt(path.length() - 1) == '/') {
            concatenatedPath = path + cleanSegment;
        } else {
            concatenatedPath = path + "/" + cleanSegment;
        }
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), concatenatedPath,
                    uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid segment [" + segment + "] for URI [" + uri + "]: " + e.getMessage(), e);
        }
    }
}
