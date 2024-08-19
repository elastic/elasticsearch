/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import static org.elasticsearch.action.support.master.AcknowledgedRequest.DEFAULT_ACK_TIMEOUT;
import static org.elasticsearch.rest.RestRequest.PATH_RESTRICTED;

public class RestUtils {

    /**
     * Sets whether we decode a '+' in an url as a space or not.
     */
    private static final boolean DECODE_PLUS_AS_SPACE = Booleans.parseBoolean(System.getProperty("es.rest.url_plus_as_space", "false"));

    public static final UnaryOperator<String> REST_DECODER = RestUtils::decodeComponent;

    public static void decodeQueryString(String s, int fromIndex, Map<String, String> params) {
        if (fromIndex < 0) {
            return;
        }
        if (fromIndex >= s.length()) {
            return;
        }

        int queryStringLength = s.contains("#") ? s.indexOf('#') : s.length();

        String name = null;
        int pos = fromIndex; // Beginning of the unprocessed region
        int i;       // End of the unprocessed region
        char c = 0;  // Current character
        for (i = fromIndex; i < queryStringLength; i++) {
            c = s.charAt(i);
            if (c == '=' && name == null) {
                if (pos != i) {
                    name = decodeQueryStringParam(s.substring(pos, i));
                }
                pos = i + 1;
            } else if (c == '&' || c == ';') {
                if (name == null && pos != i) {
                    // We haven't seen an `=' so far but moved forward.
                    // Must be a param of the form '&a&' so add it with
                    // an empty value.
                    addParam(params, decodeQueryStringParam(s.substring(pos, i)), "");
                } else if (name != null) {
                    addParam(params, name, decodeQueryStringParam(s.substring(pos, i)));
                    name = null;
                }
                pos = i + 1;
            }
        }

        if (pos != i) {  // Are there characters we haven't dealt with?
            if (name == null) {     // Yes and we haven't seen any `='.
                addParam(params, decodeQueryStringParam(s.substring(pos, i)), "");
            } else {                // Yes and this must be the last value.
                addParam(params, name, decodeQueryStringParam(s.substring(pos, i)));
            }
        } else if (name != null) {  // Have we seen a name without value?
            addParam(params, name, "");
        }
    }

    private static String decodeQueryStringParam(final String s) {
        return decodeComponent(s, StandardCharsets.UTF_8, true);
    }

    private static void addParam(Map<String, String> params, String name, String value) {
        if (PATH_RESTRICTED.equalsIgnoreCase(name)) {
            throw new IllegalArgumentException("parameter [" + PATH_RESTRICTED + "] is reserved and may not set");
        }
        params.put(name, value);
    }

    /**
     * Decodes a bit of an URL encoded by a browser.
     * <p>
     * This is equivalent to calling {@link #decodeComponent(String, Charset, boolean)}
     * with the UTF-8 charset (recommended to comply with RFC 3986, Section 2).
     *
     * @param s The string to decode (can be empty).
     * @return The decoded string, or {@code s} if there's nothing to decode.
     *         If the string to decode is {@code null}, returns an empty string.
     * @throws IllegalArgumentException if the string contains a malformed
     *                                  escape sequence.
     */
    public static String decodeComponent(final String s) {
        return decodeComponent(s, StandardCharsets.UTF_8, DECODE_PLUS_AS_SPACE);
    }

    /**
     * Decodes a bit of an URL encoded by a browser.
     * <p>
     * The string is expected to be encoded as per RFC 3986, Section 2.
     * This is the encoding used by JavaScript functions {@code encodeURI}
     * and {@code encodeURIComponent}, but not {@code escape}.  For example
     * in this encoding, &eacute; (in Unicode {@code U+00E9} or in UTF-8
     * {@code 0xC3 0xA9}) is encoded as {@code %C3%A9} or {@code %c3%a9}.
     * <p>
     * This is essentially equivalent to calling
     * <code>{@link java.net.URLDecoder URLDecoder}.{@link
     * java.net.URLDecoder#decode(String, String)}</code>
     * except that it's over 2x faster and generates less garbage for the GC.
     * Actually this function doesn't allocate any memory if there's nothing
     * to decode, the argument itself is returned.
     *
     * @param s           The string to decode (can be empty).
     * @param charset     The charset to use to decode the string (should really
     *                    be {@link StandardCharsets#UTF_8}.
     * @param plusAsSpace Whether to decode a {@code '+'} to a single space {@code ' '}
     * @return The decoded string, or {@code s} if there's nothing to decode.
     *         If the string to decode is {@code null}, returns an empty string.
     * @throws IllegalArgumentException if the string contains a malformed
     *                                  escape sequence.
     */
    private static String decodeComponent(final String s, final Charset charset, boolean plusAsSpace) {
        if (s == null) {
            return "";
        }
        final int size = s.length();
        if (decodingNeeded(s, size, plusAsSpace) == false) {
            return s;
        }
        final byte[] buf = new byte[size];
        int pos = decode(s, size, buf, plusAsSpace);
        return new String(buf, 0, pos, charset);
    }

    private static boolean decodingNeeded(String s, int size, boolean plusAsSpace) {
        if (Strings.isEmpty(s)) {
            return false;
        }
        int num = Math.min(s.length(), size);
        for (int i = 0; i < num; i++) {
            final char c = s.charAt(i);
            if (c == '%' || (plusAsSpace && c == '+')) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("fallthrough")
    private static int decode(String s, int size, byte[] buf, boolean plusAsSpace) {
        int pos = 0;  // position in `buf'.
        for (int i = 0; i < size; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '+':
                    buf[pos++] = (byte) (plusAsSpace ? ' ' : '+');  // "+" -> " "
                    break;
                case '%':
                    if (i == size - 1) {
                        throw new IllegalArgumentException("unterminated escape sequence at end of string: " + s);
                    }
                    c = s.charAt(++i);
                    if (c == '%') {
                        buf[pos++] = '%';  // "%%" -> "%"
                        break;
                    } else if (i == size - 1) {
                        throw new IllegalArgumentException("partial escape sequence at end of string: " + s);
                    }
                    c = decodeHexNibble(c);
                    final char c2 = decodeHexNibble(s.charAt(++i));
                    if (c == Character.MAX_VALUE || c2 == Character.MAX_VALUE) {
                        throw new IllegalArgumentException(
                            "invalid escape sequence `%" + s.charAt(i - 1) + s.charAt(i) + "' at index " + (i - 2) + " of: " + s
                        );
                    }
                    c = (char) (c * 16 + c2);
                    // Fall through.
                default:
                    buf[pos++] = (byte) c;
                    break;
            }
        }
        return pos;
    }

    /**
     * Helper to decode half of a hexadecimal number from a string.
     *
     * @param c The ASCII character of the hexadecimal number to decode.
     *          Must be in the range {@code [0-9a-fA-F]}.
     * @return The hexadecimal value represented in the ASCII character
     *         given, or {@link Character#MAX_VALUE} if the character is invalid.
     */
    private static char decodeHexNibble(final char c) {
        if ('0' <= c && c <= '9') {
            return (char) (c - '0');
        } else if ('a' <= c && c <= 'f') {
            return (char) (c - 'a' + 10);
        } else if ('A' <= c && c <= 'F') {
            return (char) (c - 'A' + 10);
        } else {
            return Character.MAX_VALUE;
        }
    }

    /**
     * Determine if CORS setting is a regex
     *
     * @return a corresponding {@link Pattern} if so and o.w. null.
     */
    public static Pattern checkCorsSettingForRegex(String corsSetting) {
        if (corsSetting == null) {
            return null;
        }
        int len = corsSetting.length();
        boolean isRegex = len > 2 && corsSetting.startsWith("/") && corsSetting.endsWith("/");

        if (isRegex) {
            return Pattern.compile(corsSetting.substring(1, corsSetting.length() - 1));
        }

        return null;
    }

    /**
     * Return the CORS setting as an array of origins.
     *
     * @param corsSetting the CORS allow origin setting as configured by the user;
     *                    should never pass null, but we check for it anyway.
     * @return an array of origins if set, otherwise {@code null}.
     */
    public static String[] corsSettingAsArray(String corsSetting) {
        if (Strings.isNullOrEmpty(corsSetting)) {
            return new String[0];
        }
        return Arrays.stream(corsSetting.split(",")).map(String::trim).toArray(String[]::new);
    }

    /**
     * Extract the trace id from the specified traceparent string.
     * @see <a href="https://www.w3.org/TR/trace-context/#traceparent-header">W3 traceparent spec</a>
     *
     * @param traceparent   The value from the {@code traceparent} HTTP header
     * @return  The trace id from the traceparent string, or {@code Optional.empty()} if it is not present.
     */
    public static Optional<String> extractTraceId(String traceparent) {
        return traceparent != null && traceparent.length() >= 55 ? Optional.of(traceparent.substring(3, 35)) : Optional.empty();
    }

    /**
     * The name of the common {@code ?master_timeout} query parameter.
     */
    public static final String REST_MASTER_TIMEOUT_PARAM = "master_timeout";

    /**
     * The default value for the common {@code ?master_timeout} query parameter.
     */
    public static final TimeValue REST_MASTER_TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(30);

    /**
     * The name of the common {@code ?timeout} query parameter.
     */
    public static final String REST_TIMEOUT_PARAM = "timeout";

    /**
     * Extract the {@code ?master_timeout} parameter from the request, imposing the common default of {@code 30s} in case the parameter is
     * missing.
     *
     * @param restRequest The request from which to extract the {@code ?master_timeout} parameter
     * @return the timeout from the request, with a default of {@link #REST_MASTER_TIMEOUT_DEFAULT} ({@code 30s}) if the request does not
     *         specify the parameter
     */
    public static TimeValue getMasterNodeTimeout(RestRequest restRequest) {
        assert restRequest != null;
        return restRequest.paramAsTime(REST_MASTER_TIMEOUT_PARAM, REST_MASTER_TIMEOUT_DEFAULT);
    }

    /**
     * Extract the {@code ?timeout} parameter from the request, imposing the common default of {@code 30s} in case the parameter is
     * missing.
     *
     * @param restRequest The request from which to extract the {@code ?timeout} parameter
     * @return the timeout from the request, with a default of {@link AcknowledgedRequest#DEFAULT_ACK_TIMEOUT} ({@code 30s}) if the request
     *         does not specify the parameter
     */
    public static TimeValue getAckTimeout(RestRequest restRequest) {
        assert restRequest != null;
        return restRequest.paramAsTime(REST_TIMEOUT_PARAM, DEFAULT_ACK_TIMEOUT);
    }

    /**
     * Extract the {@code ?timeout} parameter from the request, returning null in case the parameter is missing.
     *
     * @param restRequest The request from which to extract the {@code ?timeout} parameter
     * @return the timeout from the request, with a default of {@code null} if the request does not specify the parameter
     */
    @Nullable
    public static TimeValue getTimeout(RestRequest restRequest) {
        assert restRequest != null;
        return restRequest.paramAsTime(REST_TIMEOUT_PARAM, null);
    }
}
