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

package org.elasticsearch.common;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 *
 */
public class Strings {

    public static final String[] EMPTY_ARRAY = new String[0];

    private static final String FOLDER_SEPARATOR = "/";

    private static final String WINDOWS_FOLDER_SEPARATOR = "\\";

    private static final String TOP_PATH = "src/test";

    private static final String CURRENT_PATH = ".";

    public static void spaceify(int spaces, String from, StringBuilder to) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FastStringReader(from))) {
            String line;
            while ((line = reader.readLine()) != null) {
                for (int i = 0; i < spaces; i++) {
                    to.append(' ');
                }
                to.append(line).append('\n');
            }
        }
    }

    /**
     * Splits a backslash escaped string on the separator.
     * <p>
     * Current backslash escaping supported:
     * <br> \n \t \r \b \f are escaped the same as a Java String
     * <br> Other characters following a backslash are produced verbatim (\c =&gt; c)
     *
     * @param s         the string to split
     * @param separator the separator to split on
     * @param decode    decode backslash escaping
     */
    public static List<String> splitSmart(String s, String separator, boolean decode) {
        ArrayList<String> lst = new ArrayList<>(2);
        StringBuilder sb = new StringBuilder();
        int pos = 0, end = s.length();
        while (pos < end) {
            if (s.startsWith(separator, pos)) {
                if (sb.length() > 0) {
                    lst.add(sb.toString());
                    sb = new StringBuilder();
                }
                pos += separator.length();
                continue;
            }

            char ch = s.charAt(pos++);
            if (ch == '\\') {
                if (!decode) sb.append(ch);
                if (pos >= end) break;  // ERROR, or let it go?
                ch = s.charAt(pos++);
                if (decode) {
                    switch (ch) {
                        case 'n':
                            ch = '\n';
                            break;
                        case 't':
                            ch = '\t';
                            break;
                        case 'r':
                            ch = '\r';
                            break;
                        case 'b':
                            ch = '\b';
                            break;
                        case 'f':
                            ch = '\f';
                            break;
                    }
                }
            }

            sb.append(ch);
        }

        if (sb.length() > 0) {
            lst.add(sb.toString());
        }

        return lst;
    }


    //---------------------------------------------------------------------
    // General convenience methods for working with Strings
    //---------------------------------------------------------------------

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
        return (str != null && str.length() > 0);
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
        return !hasLength(str);
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
        if (!hasLength(str)) {
            return false;
        }
        int strLen = str.length();
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
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
        return hasText((CharSequence) str);
    }

    /**
     * Trim all occurrences of the supplied leading character from the given String.
     *
     * @param str              the String to check
     * @param leadingCharacter the leading character to be trimmed
     * @return the trimmed String
     */
    public static String trimLeadingCharacter(String str, char leadingCharacter) {
        if (!hasLength(str)) {
            return str;
        }
        StringBuilder sb = new StringBuilder(str);
        while (sb.length() > 0 && sb.charAt(0) == leadingCharacter) {
            sb.deleteCharAt(0);
        }
        return sb.toString();
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
     * Count the occurrences of the substring in string s.
     *
     * @param str string to search in. Return 0 if this is null.
     * @param sub string to search for. Return 0 if this is null.
     */
    public static int countOccurrencesOf(String str, String sub) {
        if (str == null || sub == null || str.length() == 0 || sub.length() == 0) {
            return 0;
        }
        int count = 0;
        int pos = 0;
        int idx;
        while ((idx = str.indexOf(sub, pos)) != -1) {
            ++count;
            pos = idx + sub.length();
        }
        return count;
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
        if (!hasLength(inString) || !hasLength(oldPattern) || newPattern == null) {
            return inString;
        }
        StringBuilder sb = new StringBuilder();
        int pos = 0; // our position in the old string
        int index = inString.indexOf(oldPattern);
        // the index of an occurrence we've found, or -1
        int patLen = oldPattern.length();
        while (index >= 0) {
            sb.append(inString.substring(pos, index));
            sb.append(newPattern);
            pos = index + patLen;
            index = inString.indexOf(oldPattern, pos);
        }
        sb.append(inString.substring(pos));
        // remember to append any characters to the right of a match
        return sb.toString();
    }

    /**
     * Delete all occurrences of the given substring.
     *
     * @param inString the original String
     * @param pattern  the pattern to delete all occurrences of
     * @return the resulting String
     */
    public static String delete(String inString, String pattern) {
        return replace(inString, pattern, "");
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
        if (!hasLength(inString) || !hasLength(charsToDelete)) {
            return inString;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inString.length(); i++) {
            char c = inString.charAt(i);
            if (charsToDelete.indexOf(c) == -1) {
                sb.append(c);
            }
        }
        return sb.toString();
    }


    //---------------------------------------------------------------------
    // Convenience methods for working with formatted Strings
    //---------------------------------------------------------------------

    /**
     * Quote the given String with single quotes.
     *
     * @param str the input String (e.g. "myString")
     * @return the quoted String (e.g. "'myString'"),
     *         or <code>null</code> if the input was <code>null</code>
     */
    public static String quote(String str) {
        return (str != null ? "'" + str + "'" : null);
    }

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
        StringBuilder sb = new StringBuilder(str.length());
        if (capitalize) {
            sb.append(Character.toUpperCase(str.charAt(0)));
        } else {
            sb.append(Character.toLowerCase(str.charAt(0)));
        }
        sb.append(str.substring(1));
        return sb.toString();
    }

    public static final Set<Character> INVALID_FILENAME_CHARS = unmodifiableSet(
            newHashSet('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ','));

    public static boolean validFileName(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (INVALID_FILENAME_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

    public static boolean validFileNameExcludingAstrix(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c != '*' && INVALID_FILENAME_CHARS.contains(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Normalize the path by suppressing sequences like "path/.." and
     * inner simple dots.
     * <p>The result is convenient for path comparison. For other uses,
     * notice that Windows separators ("\") are replaced by simple slashes.
     *
     * @param path the original path
     * @return the normalized path
     */
    public static String cleanPath(String path) {
        if (path == null) {
            return null;
        }
        String pathToUse = replace(path, WINDOWS_FOLDER_SEPARATOR, FOLDER_SEPARATOR);

        // Strip prefix from path to analyze, to not treat it as part of the
        // first path element. This is necessary to correctly parse paths like
        // "file:core/../core/io/Resource.class", where the ".." should just
        // strip the first "core" directory while keeping the "file:" prefix.
        int prefixIndex = pathToUse.indexOf(":");
        String prefix = "";
        if (prefixIndex != -1) {
            prefix = pathToUse.substring(0, prefixIndex + 1);
            pathToUse = pathToUse.substring(prefixIndex + 1);
        }
        if (pathToUse.startsWith(FOLDER_SEPARATOR)) {
            prefix = prefix + FOLDER_SEPARATOR;
            pathToUse = pathToUse.substring(1);
        }

        String[] pathArray = delimitedListToStringArray(pathToUse, FOLDER_SEPARATOR);
        List<String> pathElements = new LinkedList<>();
        int tops = 0;

        for (int i = pathArray.length - 1; i >= 0; i--) {
            String element = pathArray[i];
            if (CURRENT_PATH.equals(element)) {
                // Points to current directory - drop it.
            } else if (TOP_PATH.equals(element)) {
                // Registering top path found.
                tops++;
            } else {
                if (tops > 0) {
                    // Merging path element with element corresponding to top path.
                    tops--;
                } else {
                    // Normal path element found.
                    pathElements.add(0, element);
                }
            }
        }

        // Remaining top paths need to be retained.
        for (int i = 0; i < tops; i++) {
            pathElements.add(0, TOP_PATH);
        }

        return prefix + collectionToDelimitedString(pathElements, FOLDER_SEPARATOR);
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
        return collection.toArray(new String[collection.size()]);
    }

    public static Set<String> splitStringByCommaToSet(final String s) {
        return splitStringToSet(s, ',');
    }

    public static String[] splitStringByCommaToArray(final String s) {
        if (s == null || s.isEmpty()) return Strings.EMPTY_ARRAY;
        else return s.split(",");
    }

    public static Set<String> splitStringToSet(final String s, final char c) {
        final char[] chars = s.toCharArray();
        int count = 1;
        for (final char x : chars) {
            if (x == c) {
                count++;
            }
        }
        final Set<String> result = new HashSet<>(count);
        final int len = chars.length;
        int start = 0;  // starting index in chars of the current substring.
        int pos = 0;    // current index in chars.
        for (; pos < len; pos++) {
            if (chars[pos] == c) {
                int size = pos - start;
                if (size > 0) { // only add non empty strings
                    result.add(new String(chars, start, size));
                }
                start = pos + 1;
            }
        }
        int size = pos - start;
        if (size > 0) {
            result.add(new String(chars, start, size));
        }
        return result;
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
        if (!hasLength(toSplit) || !hasLength(delimiter)) {
            return null;
        }
        int offset = toSplit.indexOf(delimiter);
        if (offset < 0) {
            return null;
        }
        String beforeDelimiter = toSplit.substring(0, offset);
        String afterDelimiter = toSplit.substring(offset + delimiter.length());
        return new String[]{beforeDelimiter, afterDelimiter};
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer.
     * Trims tokens and omits empty tokens.
     * <p>The given delimiters string is supposed to consist of any number of
     * delimiter characters. Each of those characters can be used to separate
     * tokens. A delimiter is always a single character; for multi-character
     * delimiters, consider using <code>delimitedListToStringArray</code>
     *
     * @param str        the String to tokenize
     * @param delimiters the delimiter characters, assembled as String
     *                   (each of those characters is individually considered as delimiter).
     * @return an array of the tokens
     * @see java.util.StringTokenizer
     * @see java.lang.String#trim()
     * @see #delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(String str, String delimiters) {
        return tokenizeToStringArray(str, delimiters, true, true);
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer.
     * <p>The given delimiters string is supposed to consist of any number of
     * delimiter characters. Each of those characters can be used to separate
     * tokens. A delimiter is always a single character; for multi-character
     * delimiters, consider using <code>delimitedListToStringArray</code>
     *
     * @param str               the String to tokenize
     * @param delimiters        the delimiter characters, assembled as String
     *                          (each of those characters is individually considered as delimiter)
     * @param trimTokens        trim the tokens via String's <code>trim</code>
     * @param ignoreEmptyTokens omit empty tokens from the result array
     *                          (only applies to tokens that are empty after trimming; StringTokenizer
     *                          will not consider subsequent delimiters as token in the first place).
     * @return an array of the tokens (<code>null</code> if the input String
     *         was <code>null</code>)
     * @see java.util.StringTokenizer
     * @see java.lang.String#trim()
     * @see #delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(
            String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {

        if (str == null) {
            return null;
        }
        StringTokenizer st = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }
        return toStringArray(tokens);
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
            return new String[0];
        }
        if (delimiter == null) {
            return new String[]{str};
        }
        List<String> result = new ArrayList<>();
        if ("".equals(delimiter)) {
            for (int i = 0; i < str.length(); i++) {
                result.add(deleteAny(str.substring(i, i + 1), charsToDelete));
            }
        } else {
            int pos = 0;
            int delPos;
            while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                result.add(deleteAny(str.substring(pos, delPos), charsToDelete));
                pos = delPos + delimiter.length();
            }
            if (str.length() > 0 && pos <= str.length()) {
                // Add rest of String, but not in case of empty input.
                result.add(deleteAny(str.substring(pos), charsToDelete));
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
        Set<String> set = new TreeSet<>();
        String[] tokens = commaDelimitedListToStringArray(str);
        set.addAll(Arrays.asList(tokens));
        return set;
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll   the Collection to display
     * @param delim  the delimiter to use (probably a ",")
     * @param prefix the String to start each element with
     * @param suffix the String to end each element with
     * @return the delimited String
     */
    public static String collectionToDelimitedString(Iterable<?> coll, String delim, String prefix, String suffix) {
        return collectionToDelimitedString(coll, delim, prefix, suffix, new StringBuilder());
    }

    public static String collectionToDelimitedString(Iterable<?> coll, String delim, String prefix, String suffix, StringBuilder sb) {
        Iterator<?> it = coll.iterator();
        while (it.hasNext()) {
            sb.append(prefix).append(it.next()).append(suffix);
            if (it.hasNext()) {
                sb.append(delim);
            }
        }
        return sb.toString();
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV)
     * String. E.g. useful for <code>toString()</code> implementations.
     *
     * @param coll  the Collection to display
     * @param delim the delimiter to use (probably a ",")
     * @return the delimited String
     */
    public static String collectionToDelimitedString(Iterable<?> coll, String delim) {
        return collectionToDelimitedString(coll, delim, "", "");
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
        return arrayToDelimitedString(arr, delim, new StringBuilder());
    }

    public static String arrayToDelimitedString(Object[] arr, String delim, StringBuilder sb) {
        if (isEmpty(arr)) {
            return "";
        }
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(arr[i]);
        }
        return sb.toString();
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

    private Strings() {
    }

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
        return CollectionUtils.isEmpty(data) ||
               data.length == 1 && ("_all".equals(data[0]) || "*".equals(data[0]));
    }

    /**
     * Return a {@link String} that is the json representation of the provided
     * {@link ToXContent}.
     */
    public static String toString(ToXContent toXContent) {
        return toString(toXContent, false);
    }

    /**
     * Return a {@link String} that is the json representation of the provided
     * {@link ToXContent}.
     * @param wrapInObject set this to true if the ToXContent instance expects to be inside an object
     */
    public static String toString(ToXContent toXContent, boolean wrapInObject) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            if (wrapInObject) {
                builder.startObject();
            }
            toXContent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            if (wrapInObject) {
                builder.endObject();
            }
            return builder.string();
        } catch (IOException e) {
            return "Error building toString out of XContent: " + ExceptionsHelper.stackTrace(e);
        }
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

    public static boolean isNullOrEmpty(@Nullable String s) {
        return s == null || s.isEmpty();
    }

    public static String coalesceToEmpty(@Nullable String s) {
        return s == null ? "" : s;
    }

    public static String padStart(String s, int minimumLength, char c) {
        if (s == null) {
            throw new NullPointerException("s");
        }
        if (s.length() >= minimumLength) {
            return s;
        } else {
            StringBuilder sb = new StringBuilder(minimumLength);
            for (int i = s.length(); i < minimumLength; i++) {
                sb.append(c);
            }

            sb.append(s);
            return sb.toString();
        }
    }

}
