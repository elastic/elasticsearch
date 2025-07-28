/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Strings {

    public static final Base64.Encoder BASE_64_NO_PADDING_URL_ENCODER = Base64.getUrlEncoder().withoutPadding();

    public static final String[] EMPTY_ARRAY = new String[0];

    // ---------------------------------------------------------------------
    // General convenience methods for working with Strings
    // ---------------------------------------------------------------------

    /**
     * Check that the given CharSequence is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a CharSequence that purely consists of whitespace.
     * <pre>
     * StringUtils.hasLength(null) = false
     * StringUtils.hasLength("") = false
     * StringUtils.hasLength(" ") = true
     * StringUtils.hasLength("Hello") = true
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is not null and has length
     * @see #hasText(String)
     */
    public static boolean hasLength(CharSequence str) {
        return (str != null && str.isEmpty() == false);
    }

    /**
     * Check that the given BytesReference is neither <code>null</code> nor of length 0
     * Note: Will return <code>true</code> for a BytesReference that purely consists of whitespace.
     *
     * @param bytesReference the BytesReference to check (may be <code>null</code>)
     * @return <code>true</code> if the BytesReference is not null and has length
     * @see #hasLength(CharSequence)
     */
    public static boolean hasLength(BytesReference bytesReference) {
        return (bytesReference != null && bytesReference.length() > 0);
    }

    /**
     * Check that the given String is neither <code>null</code> nor of length 0.
     * Note: Will return <code>true</code> for a String that purely consists of whitespace.
     *
     * @param str the String to check (may be <code>null</code>)
     * @return <code>true</code> if the String is not null and has length
     * @see #hasLength(CharSequence)
     */
    public static boolean hasLength(String str) {
        return hasLength((CharSequence) str);
    }

    /**
     * Check that the given CharSequence is either <code>null</code> or of length 0.
     * Note: Will return <code>false</code> for a CharSequence that purely consists of whitespace.
     * <pre>
     * StringUtils.isEmpty(null) = true
     * StringUtils.isEmpty("") = true
     * StringUtils.isEmpty(" ") = false
     * StringUtils.isEmpty("Hello") = false
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is either null or has a zero length
     */
    public static boolean isEmpty(CharSequence str) {
        return hasLength(str) == false;
    }

    /**
     * Check whether the given CharSequence has actual text.
     * More specifically, returns <code>true</code> if the string not <code>null</code>,
     * its length is greater than 0, and it contains at least one non-whitespace character.
     * <pre>
     * StringUtils.hasText(null) = false
     * StringUtils.hasText("") = false
     * StringUtils.hasText(" ") = false
     * StringUtils.hasText("12345") = true
     * StringUtils.hasText(" 12345 ") = true
     * </pre>
     *
     * @param str the CharSequence to check (may be <code>null</code>)
     * @return <code>true</code> if the CharSequence is not <code>null</code>,
     *         its length is greater than 0, and it does not contain whitespace only
     * @see java.lang.Character#isWhitespace
     */
    public static boolean hasText(CharSequence str) {
        if (hasLength(str) == false) {
            return false;
        }
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(str.charAt(i)) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given String has actual text.
     * More specifically, returns <code>true</code> if the string not <code>null</code>,
     * its length is greater than 0, and it contains at least one non-whitespace character.
     *
     * @param str the String to check (may be <code>null</code>)
     * @return <code>true</code> if the String is not <code>null</code>, its length is
     *         greater than 0, and it does not contain whitespace only
     * @see #hasText(CharSequence)
     */
    public static boolean hasText(String str) {
        return isNullOrBlank(str) == false;
    }

    /**
     * Trim all occurrences of the supplied leading character from the given String.
     *
     * @param str              the String to check
     * @param leadingCharacter the leading character to be trimmed
     * @return the trimmed String
     */
    public static String trimLeadingCharacter(String str, char leadingCharacter) {
        if (hasLength(str) == false) {
            return str;
        }
        int i = 0;
        while (i < str.length() && str.charAt(i) == leadingCharacter) {
            i++;
        }
        return str.substring(i);
    }

    /**
     * Test whether the given string matches the given substring
     * at the given index.
     *
     * @param str       the original string (or StringBuilder)
     * @param index     the index in the original string to start matching against
     * @param substring the substring to match at the given index
     */
    public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Replace all occurrences of a substring within a string with
     * another string.
     *
     * @param inString   String to examine
     * @param oldPattern String to replace
     * @param newPattern String to insert
     * @return a String with the replacements
     */
    public static String replace(String inString, String oldPattern, String newPattern) {
        if (hasLength(inString) == false || hasLength(oldPattern) == false || newPattern == null) {
            return inString;
        }
        StringBuilder sb = new StringBuilder();
        int pos = 0; // our position in the old string
        int index = inString.indexOf(oldPattern);
        // the index of an occurrence we've found, or -1
        int patLen = oldPattern.length();
        while (index >= 0) {
            sb.append(inString, pos, index);
            sb.append(newPattern);
            pos = index + patLen;
            index = inString.indexOf(oldPattern, pos);
        }
        sb.append(inString.substring(pos));
        // remember to append any characters to the right of a match
        return sb.toString();
    }

    /**
     * Delete any character in a given String.
     *
     * @param inString      the original String
     * @param charsToDelete a set of characters to delete.
     *                      E.g. "az\n" will delete 'a's, 'z's and new lines.
     * @return the resulting String
     */
    public static String deleteAny(String inString, String charsToDelete) {
        return inString != null ? deleteAny((CharSequence) inString, charsToDelete).toString() : null;
    }

    /**
     * Delete any character in a given CharSequence.
     *
     * @param inString      the original CharSequence
     * @param charsToDelete a set of characters to delete.
     *                      E.g. "az\n" will delete 'a's, 'z's and new lines.
     * @return the resulting CharSequence
     */
    public static CharSequence deleteAny(CharSequence inString, String charsToDelete) {
        if (hasLength(inString) == false || hasLength(charsToDelete) == false) {
            return inString;
        }
        StringBuilder sb = new StringBuilder(inString.length());
        for (int i = 0; i < inString.length(); i++) {
            char c = inString.charAt(i);
            if (charsToDelete.indexOf(c) == -1) {
                sb.append(c);
            }
        }
        return sb;
    }

    // ---------------------------------------------------------------------
    // Convenience methods for working with formatted Strings
    // ---------------------------------------------------------------------

    /**
     * Capitalize a <code>String</code>, changing the first letter to
     * upper case as per {@link Character#toUpperCase(char)}.
     * No other letters are changed.
     *
     * @param str the String to capitalize, may be <code>null</code>
     * @return the capitalized String, <code>null</code> if null
     */
    public static String capitalize(String str) {
        return changeFirstCharacterCase(str, true);
    }

    private static String changeFirstCharacterCase(String str, boolean capitalize) {
        if (str == null || str.length() == 0) {
            return str;
        }
        char newChar = capitalize ? Character.toUpperCase(str.charAt(0)) : Character.toLowerCase(str.charAt(0));
        if (newChar == str.charAt(0)) {
            return str; // nothing changed
        }

        return newChar + str.substring(1);
    }

    // Visible for testing
    static final Set<Character> INVALID_CHARS = Set.of('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',');

    public static final String INVALID_FILENAME_CHARS = INVALID_CHARS.stream()
        .sorted()
        .map(c -> "'" + c + "'")
        .collect(Collectors.joining(",", "[", "]"));

    public static final Pattern INVALID_FILENAME_CHARS_REGEX = Pattern.compile(
        "[" + INVALID_CHARS.stream().map(Objects::toString).map(Pattern::quote).collect(Collectors.joining()) + "]+"
    );

    public static boolean validFileName(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            if (isInvalidFileNameCharacter(fileName.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean validFileNameExcludingAstrix(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c != '*' && isInvalidFileNameCharacter(c)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isInvalidFileNameCharacter(char c) {
        return switch (c) {
            case '\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',' -> true;
            default -> false;
        };
    }

    /**
     * Copy the given Collection into a String array.
     * The Collection must contain String elements only.
     *
     * @param collection the Collection to copy
     * @return the String array (<code>null</code> if the passed-in
     *         Collection was <code>null</code>)
     */
    public static String[] toStringArray(Collection<String> collection) {
        if (collection == null) {
            return null;
        }
        return collection.toArray(String[]::new);
    }

    /**
     * Concatenate two string arrays into a third
     */
    public static String[] concatStringArrays(String[] first, String[] second) {
        if (first == null && second == null) {
            return Strings.EMPTY_ARRAY;
        }
        if (first == null || first.length == 0) {
            return second;
        }
        if (second == null || second.length == 0) {
            return first;
        }
        String[] concat = new String[first.length + second.length];
        System.arraycopy(first, 0, concat, 0, first.length);
        System.arraycopy(second, 0, concat, first.length, second.length);
        return concat;
    }

    /**
     * Tokenize the specified string by commas to a set, trimming whitespace and ignoring empty tokens.
     *
     * @param s the string to tokenize
     * @return the set of tokens
     */
    public static Set<String> tokenizeByCommaToSet(final String s) {
        if (s == null) return Collections.emptySet();
        return tokenizeToCollection(s, ",", HashSet::new);
    }

    /**
     * Split the specified string by commas to an array.
     *
     * @param s the string to split
     * @return the array of split values
     * @see String#split(String)
     */
    public static String[] splitStringByCommaToArray(final String s) {
        if (s == null || s.isEmpty()) return Strings.EMPTY_ARRAY;
        else return s.split(",");
    }

    /**
     * Split a String at the first occurrence of the delimiter.
     * Does not include the delimiter in the result.
     *
     * @param toSplit   the string to split
     * @param delimiter to split the string up with
     * @return a two element array with index 0 being before the delimiter, and
     *         index 1 being after the delimiter (neither element includes the delimiter);
     *         or <code>null</code> if the delimiter wasn't found in the given input String
     */
    public static String[] split(String toSplit, String delimiter) {
        if (hasLength(toSplit) == false || hasLength(delimiter) == false) {
            return null;
        }
        int offset = toSplit.indexOf(delimiter);
        if (offset < 0) {
            return null;
        }
        String beforeDelimiter = toSplit.substring(0, offset);
        String afterDelimiter = toSplit.substring(offset + delimiter.length());
        return new String[] { beforeDelimiter, afterDelimiter };
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer.
     * Trims tokens and omits empty tokens.
     * <p>The given delimiters string is supposed to consist of any number of
     * delimiter characters. Each of those characters can be used to separate
     * tokens. A delimiter is always a single character; for multi-character
     * delimiters, consider using <code>delimitedListToStringArray</code>
     *
     * @param s        the String to tokenize
     * @param delimiters the delimiter characters, assembled as String
     *                   (each of those characters is individually considered as delimiter).
     * @return an array of the tokens
     * @see java.util.StringTokenizer
     * @see java.lang.String#trim()
     * @see #delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(final String s, final String delimiters) {
        if (s == null) {
            return EMPTY_ARRAY;
        }
        return toStringArray(tokenizeToCollection(s, delimiters, ArrayList::new));
    }

    /**
     * Tokenizes the specified string to a collection using the specified delimiters as the token delimiters. This method trims whitespace
     * from tokens and ignores empty tokens.
     *
     * @param s          the string to tokenize.
     * @param delimiters the token delimiters
     * @param supplier   a collection supplier
     * @param <T>        the type of the collection
     * @return the tokens
     * @see java.util.StringTokenizer
     */
    private static <T extends Collection<String>> T tokenizeToCollection(
        final String s,
        final String delimiters,
        final Supplier<T> supplier
    ) {
        if (s == null) {
            return null;
        }
        final StringTokenizer tokenizer = new StringTokenizer(s, delimiters);
        final T tokens = supplier.get();
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken().trim();
            if (token.length() > 0) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str       the input String
     * @param delimiter the delimiter between elements (this is a single delimiter,
     *                  rather than a bunch individual delimiter characters)
     * @return an array of the tokens in the list
     * @see #tokenizeToStringArray
     */
    public static String[] delimitedListToStringArray(String str, String delimiter) {
        return delimitedListToStringArray(str, delimiter, null);
    }

    /**
     * Take a String which is a delimited list and convert it to a String array.
     * <p>A single delimiter can consists of more than one character: It will still
     * be considered as single delimiter string, rather than as bunch of potential
     * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
     *
     * @param str           the input String
     * @param delimiter     the delimiter between elements (this is a single delimiter,
     *                      rather than a bunch individual delimiter characters)
     * @param charsToDelete a set of characters to delete. Useful for deleting unwanted
     *                      line breaks: e.g. "\r\n\f" will delete all new lines and line feeds in a String.
     * @return an array of the tokens in the list
     * @see #tokenizeToStringArray
     */
    public static String[] delimitedListToStringArray(String str, String delimiter, String charsToDelete) {
        if (str == null) {
            return EMPTY_ARRAY;
        }
        if (delimiter == null) {
            return new String[] { str };
        }
        List<String> result;
        if (delimiter.isEmpty()) {
            // split on every character
            result = new ArrayList<>(str.length());
            if (charsToDelete == null) {
                charsToDelete = "";
            }
            for (int i = 0; i < str.length(); i++) {
                if (charsToDelete.indexOf(str.charAt(i)) == -1) {
                    result.add(Character.toString(str.charAt(i)));
                } else {
                    result.add("");
                }
            }
        } else {
            result = new ArrayList<>();
            int pos = 0;
            int delPos;
            while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                result.add(deleteAny(str.subSequence(pos, delPos), charsToDelete).toString());
                pos = delPos + delimiter.length();
            }
            if (str.length() > 0 && pos <= str.length()) {
                // Add rest of String, but not in case of empty input.
                result.add(deleteAny(str.subSequence(pos, str.length()), charsToDelete).toString());
            }
        }
        return toStringArray(result);
    }

    /**
     * Convert a CSV list into an array of Strings.
     *
     * @param str the input String
     * @return an array of Strings, or the empty array in case of empty input
     */
    public static String[] commaDelimitedListToStringArray(String str) {
        return delimitedListToStringArray(str, ",");
    }

    /**
     * Convenience method to convert a CSV string list to a set.
     * Note that this will suppress duplicates.
     *
     * @param str the input String
     * @return a Set of String entries in the list
     */
    public static Set<String> commaDelimitedListToSet(String str) {
        String[] tokens = commaDelimitedListToStringArray(str);
        return new TreeSet<>(Arrays.asList(tokens));
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll   the Collection to display
     * @param delimiter  the delimiter to use (probably a ",")
     * @return the delimited String
     */
    public static String collectionToDelimitedString(Iterable<?> coll, String delimiter) {
        StringBuilder sb = new StringBuilder();
        collectionToDelimitedString(coll, delimiter, sb);
        return sb.toString();
    }

    public static void collectionToDelimitedString(Iterable<?> coll, String delimiter, StringBuilder sb) {
        Iterator<?> it = coll.iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(delimiter);
            }
        }
    }

    /**
     * Converts a collection of items to a string like {@link #collectionToDelimitedString(Iterable, String, StringBuilder)}
     * except that it stops if the string gets too long and just indicates how many items were omitted.
     *
     * @param coll        the collection of items to display
     * @param delimiter   the delimiter to write between the items (e.g. {@code ","})
     * @param appendLimit if this many characters have been appended to the string and there are still items to display then the remaining
     *                    items are omitted
     */
    public static void collectionToDelimitedStringWithLimit(Iterable<?> coll, String delimiter, int appendLimit, StringBuilder sb) {
        final var boundedDelimitedStringCollector = new BoundedDelimitedStringCollector(sb, delimiter, appendLimit);
        coll.forEach(boundedDelimitedStringCollector::appendItem);
        boundedDelimitedStringCollector.finish();
    }

    /**
     * Collects a sequence of objects into a delimited string, dropping objects once the string reaches a certain maximum length. Similar to
     * {@link #collectionToDelimitedStringWithLimit} except that this doesn't need the collection of items to be provided up front.
     */
    public static final class BoundedDelimitedStringCollector {
        private final StringBuilder stringBuilder;
        private final String delimiter;
        private final long lengthLimit;
        private int count = 0;
        private int omitted = 0;

        public BoundedDelimitedStringCollector(StringBuilder stringBuilder, String delimiter, int appendLimit) {
            this.stringBuilder = stringBuilder;
            this.delimiter = delimiter;
            this.lengthLimit = stringBuilder.length() + appendLimit; // long to avoid overflow
        }

        /**
         * Add the given item's string representation to the string, with a delimiter if necessary and surrounded by the given prefix and
         * suffix, as long as the string is not already too long.
         */
        public void appendItem(Object item) {
            count += 1;
            if (omitted > 0) {
                omitted += 1;
                return;
            }
            if (count > 1) {
                stringBuilder.append(delimiter);
            }
            if (stringBuilder.length() > lengthLimit) {
                omitted += 1;
                stringBuilder.append("..."); // indicate there are some omissions, just in case the caller forgets to call finish()
                return;
            }
            stringBuilder.append(item);
        }

        /**
         * Complete the collection, adding to the string a summary of omitted objects, if any.
         */
        public void finish() {
            if (omitted > 0) {
                stringBuilder.append(" (").append(count).append(" in total, ").append(omitted).append(" omitted)");
            }
        }
    }

    /**
     * Convenience method to return a Collection as a CSV String.
     * E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll the Collection to display
     * @return the delimited String
     */
    public static String collectionToCommaDelimitedString(Iterable<?> coll) {
        return collectionToDelimitedString(coll, ",");
    }

    /**
     * Convenience method to return a String array as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param arr   the array to display
     * @param delim the delimiter to use (probably a ",")
     * @return the delimited String
     */
    public static String arrayToDelimitedString(Object[] arr, String delim) {
        StringBuilder sb = new StringBuilder();
        arrayToDelimitedString(arr, delim, sb);
        return sb.toString();
    }

    public static void arrayToDelimitedString(Object[] arr, String delim, StringBuilder sb) {
        if (isEmpty(arr)) {
            return;
        }
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(arr[i]);
        }
    }

    /**
     * Convenience method to return a String array as a CSV String.
     * E.g. useful for <code>toString()</code> implementations.
     *
     * @param arr the array to display
     * @return the delimited String
     */
    public static String arrayToCommaDelimitedString(Object[] arr) {
        return arrayToDelimitedString(arr, ",");
    }

    /**
     * Format the double value with a single decimal points, trimming trailing '.0'.
     */
    public static String format1Decimals(double value, String suffix) {
        String p = String.valueOf(value);
        int ix = p.indexOf('.') + 1;
        int ex = p.indexOf('E');
        char fraction = p.charAt(ix);
        if (fraction == '0') {
            if (ex != -1) {
                return p.substring(0, ix - 1) + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix - 1) + suffix;
            }
        } else {
            if (ex != -1) {
                return p.substring(0, ix) + fraction + p.substring(ex) + suffix;
            } else {
                return p.substring(0, ix) + fraction + suffix;
            }
        }
    }

    /**
     * Determine whether the given array is empty:
     * i.e. <code>null</code> or of zero length.
     *
     * @param array the array to check
     */
    private static boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }

    private Strings() {}

    public static byte[] toUTF8Bytes(CharSequence charSequence) {
        return toUTF8Bytes(charSequence, new BytesRefBuilder());
    }

    public static byte[] toUTF8Bytes(CharSequence charSequence, BytesRefBuilder spare) {
        spare.copyChars(charSequence);
        return Arrays.copyOf(spare.bytes(), spare.length());
    }

    /**
     * Return substring(beginIndex, endIndex) that is impervious to string length.
     */
    public static String substring(String s, int beginIndex, int endIndex) {
        if (s == null) {
            return s;
        }

        int realEndIndex = s.length() > 0 ? s.length() - 1 : 0;

        if (endIndex > realEndIndex) {
            return s.substring(beginIndex);
        } else {
            return s.substring(beginIndex, endIndex);
        }
    }

    /**
     * If an array only consists of zero or one element, which is "*" or "_all" return an empty array
     * which is usually used as everything
     */
    public static boolean isAllOrWildcard(String[] data) {
        return CollectionUtils.isEmpty(data) || data.length == 1 && isAllOrWildcard(data[0]);
    }

    /**
     * Returns `true` if the string is `_all` or `*`.
     */
    public static boolean isAllOrWildcard(String data) {
        return "_all".equals(data) || "*".equals(data);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. The content is not pretty-printed
     * nor human readable.
     */
    public static String toString(ToXContent toXContent) {
        return toString(toXContent, false, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ChunkedToXContent}.
     * @deprecated don't add usages of this method, it will be removed eventually
     * TODO: remove this method, it makes no sense to turn potentially very large chunked xcontent instances into a string
     */
    @Deprecated
    public static String toString(ChunkedToXContent chunkedToXContent) {
        return toString(chunkedToXContent, false, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * The content is not pretty-printed nor human readable.
     */
    public static String toString(ToXContent toXContent, ToXContent.Params params) {
        return toString(toXContent, params, false, false);
    }

    /**
     * Returns a string representation of the builder (only applicable for text based xcontent).
     * @param xContentBuilder builder containing an object to converted to a string
     */
    public static String toString(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream baos) {
            return baos.toString(StandardCharsets.UTF_8);
        } else {
            return ((BytesStream) stream).bytes().utf8ToString();
        }
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed. Allows to control whether the outputted
     * json needs to be pretty printed and human readable.
     *
     */
    public static String toString(ToXContent toXContent, boolean pretty, boolean human) {
        return toString(toXContent, ToXContent.EMPTY_PARAMS, pretty, human);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ChunkedToXContent}.
     * Allows to control whether the outputted json needs to be pretty printed and human readable.
     * @deprecated don't add usages of this method, it will be removed eventually
     * TODO: remove this method, it makes no sense to turn potentially very large chunked xcontent instances into a string
     */
    @Deprecated
    public static String toString(ChunkedToXContent chunkedToXContent, boolean pretty, boolean human) {
        return toString(ChunkedToXContent.wrapAsToXContent(chunkedToXContent), pretty, human);
    }

    /**
     * Return a {@link String} that is the json representation of the provided {@link ToXContent}.
     * Wraps the output into an anonymous object if needed.
     * Allows to configure the params.
     * Allows to control whether the outputted json needs to be pretty printed and human readable.
     */
    public static String toString(ToXContent toXContent, ToXContent.Params params, boolean pretty, boolean human) {
        try {
            XContentBuilder builder = createBuilder(pretty, human);
            if (toXContent.isFragment()) {
                builder.startObject();
            }
            toXContent.toXContent(builder, params);
            if (toXContent.isFragment()) {
                builder.endObject();
            }
            return toString(builder);
        } catch (IOException e) {
            try {
                XContentBuilder builder = createBuilder(pretty, human);
                builder.startObject();
                builder.field("error", "error building toString out of XContent: " + e.getMessage());
                builder.field("stack_trace", ExceptionsHelper.stackTrace(e));
                builder.endObject();
                return toString(builder);
            } catch (IOException e2) {
                throw new ElasticsearchException("cannot generate error message for deserialization", e);
            }
        }
    }

    private static XContentBuilder createBuilder(boolean pretty, boolean human) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        if (pretty) {
            builder.prettyPrint();
        }
        if (human) {
            builder.humanReadable(true);
        }
        return builder;
    }

    /**
     * Truncates string to a length less than length. Backtracks to throw out
     * high surrogates.
     */
    public static String cleanTruncate(String s, int length) {
        if (s == null) {
            return s;
        }
        /*
         * Its pretty silly for you to truncate to 0 length but just in case
         * someone does this shouldn't break.
         */
        if (length == 0) {
            return "";
        }
        if (length >= s.length()) {
            return s;
        }
        if (Character.isHighSurrogate(s.charAt(length - 1))) {
            length--;
        }
        return s.substring(0, length);
    }

    /**
     * Checks that the supplied string is neither null nor empty, per {@link #isNullOrEmpty(String)}.
     * If this check fails, then an {@link IllegalArgumentException} is thrown with the supplied message.
     *
     * @param str the <code>String</code> to check
     * @param message the exception message to use if {@code str} is null or empty
     * @return the supplied {@code str}
     */
    public static String requireNonEmpty(String str, String message) {
        if (isNullOrEmpty(str)) {
            throw new IllegalArgumentException(message);
        }
        return str;
    }

    /**
     * Checks that the supplied string is neither null nor blank, per {@link #isNullOrBlank(String)}.
     * If this check fails, then an {@link IllegalArgumentException} is thrown with the supplied message.
     *
     * @param str the <code>String</code> to check
     * @param message the exception message to use if {@code str} is null or blank
     * @return the supplied {@code str}
     */
    public static String requireNonBlank(String str, String message) {
        if (isNullOrBlank(str)) {
            throw new IllegalArgumentException(message);
        }
        return str;
    }

    public static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    public static boolean isNullOrBlank(@Nullable String s) {
        return s == null || s.isBlank();
    }

    public static String padStart(String s, int minimumLength, char c) {
        Objects.requireNonNull(s, "s");
        if (s.length() >= minimumLength) {
            return s;
        } else {
            return Character.toString(c).repeat(minimumLength - s.length()) + s;
        }
    }

    public static String toLowercaseAscii(String in) {
        return in.codePoints()
            .map(cp -> cp <= 128 ? Character.toLowerCase(cp) : cp)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    /**
     * Alias for {@link org.elasticsearch.core.Strings#format}
     */
    public static String format(String format, Object... args) {
        return org.elasticsearch.core.Strings.format(format, args);
    }

    public static String stripDisallowedChars(String string) {
        return INVALID_FILENAME_CHARS_REGEX.matcher(string).replaceAll("");
    }
}
