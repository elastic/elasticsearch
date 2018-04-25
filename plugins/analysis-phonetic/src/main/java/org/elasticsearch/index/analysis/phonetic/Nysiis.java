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

package org.elasticsearch.index.analysis.phonetic;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.StringEncoder;

import java.util.regex.Pattern;

/**
 *
 * Taken from commons-codec trunk (unreleased yet)
 *
 * Encodes a string into a NYSIIS value. NYSIIS is an encoding used to relate
 * similar names, but can also be used as a general purpose scheme to find word
 * with similar phonemes.
 *
 * <p> NYSIIS features an accuracy increase of 2.7% over the traditional Soundex
 * algorithm. </p>
 *
 * <p>Algorithm description:
 * <pre>
 * 1. Transcode first characters of name
 *   1a. MAC -&gt;   MCC
 *   1b. KN  -&gt;   NN
 *   1c. K   -&gt;   C
 *   1d. PH  -&gt;   FF
 *   1e. PF  -&gt;   FF
 *   1f. SCH -&gt;   SSS
 * 2. Transcode last characters of name
 *   2a. EE, IE          -&gt;   Y
 *   2b. DT,RT,RD,NT,ND  -&gt;   D
 * 3. First character of key = first character of name
 * 4. Transcode remaining characters by following these rules, incrementing by one character each time
 *   4a. EV  -&gt;   AF  else A,E,I,O,U -&gt; A
 *   4b. Q   -&gt;   G
 *   4c. Z   -&gt;   S
 *   4d. M   -&gt;   N
 *   4e. KN  -&gt;   N   else K -&gt; C
 *   4f. SCH -&gt;   SSS
 *   4g. PH  -&gt;   FF
 *   4h. H   -&gt;   If previous or next is nonvowel, previous
 *   4i. W   -&gt;   If previous is vowel, previous
 *   4j. Add current to key if current != last key character
 * 5. If last character is S, remove it
 * 6. If last characters are AY, replace with Y
 * 7. If last character is A, remove it
 * 8. Collapse all strings of repeated characters
 * 9. Add original first character of name as first character of key
 * </pre>
 *
 * @see <a href="http://en.wikipedia.org/wiki/NYSIIS">NYSIIS on Wikipedia</a>
 * @see <a href="http://www.dropby.com/NYSIIS.html">NYSIIS on dropby.com</a>
 *
 */
public class Nysiis implements StringEncoder {

    private static final char[] CHARS_A = new char[]{'A'};
    private static final char[] CHARS_AF = new char[]{'A', 'F'};
    private static final char[] CHARS_C = new char[]{'C'};
    private static final char[] CHARS_FF = new char[]{'F', 'F'};
    private static final char[] CHARS_G = new char[]{'G'};
    private static final char[] CHARS_N = new char[]{'N'};
    private static final char[] CHARS_NN = new char[]{'N', 'N'};
    private static final char[] CHARS_S = new char[]{'S'};
    private static final char[] CHARS_SSS = new char[]{'S', 'S', 'S'};
    private static final Pattern PAT_MAC = Pattern.compile("^MAC");
    private static final Pattern PAT_KN = Pattern.compile("^KN");
    private static final Pattern PAT_K = Pattern.compile("^K");
    private static final Pattern PAT_PH_PF = Pattern.compile("^(PH|PF)");
    private static final Pattern PAT_SCH = Pattern.compile("^SCH");
    private static final Pattern PAT_EE_IE = Pattern.compile("(EE|IE)$");
    private static final Pattern PAT_DT_ETC = Pattern.compile("(DT|RT|RD|NT|ND)$");
    private static final char SPACE = ' ';
    private static final int TRUE_LENGTH = 6;

    /**
     * Tests if the given character is a vowel.
     *
     * @param c the character to test
     * @return {@code true} if the character is a vowel, {@code false} otherwise
     */
    private static boolean isVowel(final char c) {
        return c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U';
    }

    /**
     * Transcodes the remaining parts of the String. The method operates on a
     * sliding window, looking at 4 characters at a time: [i-1, i, i+1, i+2].
     *
     * @param prev the previous character
     * @param curr the current character
     * @param next the next character
     * @param aNext the after next character
     * @return a transcoded array of characters, starting from the current
     * position
     */
    private static char[] transcodeRemaining(final char prev, final char curr, final char next, final char aNext) {
        // 1. EV -> AF
        if (curr == 'E' && next == 'V') {
            return CHARS_AF;
        }

        // A, E, I, O, U -> A
        if (isVowel(curr)) {
            return CHARS_A;
        }

        // 2. Q -> G, Z -> S, M -> N
        if (curr == 'Q') {
            return CHARS_G;
        } else if (curr == 'Z') {
            return CHARS_S;
        } else if (curr == 'M') {
            return CHARS_N;
        }

        // 3. KN -> NN else K -> C
        if (curr == 'K') {
            if (next == 'N') {
                return CHARS_NN;
            } else {
                return CHARS_C;
            }
        }

        // 4. SCH -> SSS
        if (curr == 'S' && next == 'C' && aNext == 'H') {
            return CHARS_SSS;
        }

        // PH -> FF
        if (curr == 'P' && next == 'H') {
            return CHARS_FF;
        }

        // 5. H -> If previous or next is a non vowel, previous.
        if (curr == 'H' && (!isVowel(prev) || !isVowel(next))) {
            return new char[]{prev};
        }

        // 6. W -> If previous is vowel, previous.
        if (curr == 'W' && isVowel(prev)) {
            return new char[]{prev};
        }

        return new char[]{curr};
    }
    /**
     * Indicates the strict mode.
     */
    private final boolean strict;

    /**
     * Creates an instance of the {@link Nysiis} encoder with strict mode
     * (original form), i.e. encoded strings have a maximum length of 6.
     */
    public Nysiis() {
        this(true);
    }

    /**
     * Create an instance of the {@link Nysiis} encoder with the specified
     * strict mode:
     *
     * <ul> <li>{@code true}: encoded strings have a maximum length of 6</li> <li>{@code false}:
     * encoded strings may have arbitrary length</li> </ul>
     *
     * @param strict the strict mode
     */
    public Nysiis(final boolean strict) {
        this.strict = strict;
    }

    /**
     * Encodes an Object using the NYSIIS algorithm. This method is provided in
     * order to satisfy the requirements of the Encoder interface, and will
     * throw an {@link EncoderException} if the supplied object is not of type
     * {@link String}.
     *
     * @param obj Object to encode
     * @return An object (or a {@link String}) containing the NYSIIS code which
     * corresponds to the given String.
     * @throws EncoderException if the parameter supplied is not of a {@link String}
     * @throws IllegalArgumentException if a character is not mapped
     */
    @Override
    public Object encode(Object obj) throws EncoderException {
        if (!(obj instanceof String)) {
            throw new EncoderException("Parameter supplied to Nysiis encode is not of type java.lang.String");
        }
        return this.nysiis((String) obj);
    }

    /**
     * Encodes a String using the NYSIIS algorithm.
     *
     * @param str A String object to encode
     * @return A Nysiis code corresponding to the String supplied
     * @throws IllegalArgumentException if a character is not mapped
     */
    @Override
    public String encode(String str) {
        return this.nysiis(str);
    }

    /**
     * Indicates the strict mode for this {@link Nysiis} encoder.
     *
     * @return {@code true} if the encoder is configured for strict mode, {@code false}
     * otherwise
     */
    public boolean isStrict() {
        return this.strict;
    }

    /**
     * Retrieves the NYSIIS code for a given String object.
     *
     * @param str String to encode using the NYSIIS algorithm
     * @return A NYSIIS code for the String supplied
     */
    public String nysiis(String str) {
        if (str == null) {
            return null;
        }

        // Use the same clean rules as Soundex
        str = clean(str);

        if (str.length() == 0) {
            return str;
        }

        // Translate first characters of name:
        // MAC -> MCC, KN -> NN, K -> C, PH | PF -> FF, SCH -> SSS
        str = PAT_MAC.matcher(str).replaceFirst("MCC");
        str = PAT_KN.matcher(str).replaceFirst("NN");
        str = PAT_K.matcher(str).replaceFirst("C");
        str = PAT_PH_PF.matcher(str).replaceFirst("FF");
        str = PAT_SCH.matcher(str).replaceFirst("SSS");

        // Translate last characters of name:
        // EE -> Y, IE -> Y, DT | RT | RD | NT | ND -> D
        str = PAT_EE_IE.matcher(str).replaceFirst("Y");
        str = PAT_DT_ETC.matcher(str).replaceFirst("D");

        // First character of key = first character of name.
        StringBuilder key = new StringBuilder(str.length());
        key.append(str.charAt(0));

        // Transcode remaining characters, incrementing by one character each time
        final char[] chars = str.toCharArray();
        final int len = chars.length;

        for (int i = 1; i < len; i++) {
            final char next = i < len - 1 ? chars[i + 1] : SPACE;
            final char aNext = i < len - 2 ? chars[i + 2] : SPACE;
            final char[] transcoded = transcodeRemaining(chars[i - 1], chars[i], next, aNext);
            System.arraycopy(transcoded, 0, chars, i, transcoded.length);

            // only append the current char to the key if it is different from the last one
            if (chars[i] != chars[i - 1]) {
                key.append(chars[i]);
            }
        }

        if (key.length() > 1) {
            char lastChar = key.charAt(key.length() - 1);

            // If last character is S, remove it.
            if (lastChar == 'S') {
                key.deleteCharAt(key.length() - 1);
                lastChar = key.charAt(key.length() - 1);
            }

            if (key.length() > 2) {
                final char last2Char = key.charAt(key.length() - 2);
                // If last characters are AY, replace with Y.
                if (last2Char == 'A' && lastChar == 'Y') {
                    key.deleteCharAt(key.length() - 2);
                }
            }

            // If last character is A, remove it.
            if (lastChar == 'A') {
                key.deleteCharAt(key.length() - 1);
            }
        }

        final String string = key.toString();
        return this.isStrict() ? string.substring(0, Math.min(TRUE_LENGTH, string.length())) : string;
    }

    static String clean(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        int len = str.length();
        char[] chars = new char[len];
        int count = 0;
        for (int i = 0; i < len; i++) {
            if (Character.isLetter(str.charAt(i))) {
                chars[count++] = str.charAt(i);
            }
        }
        if (count == len) {
            return str.toUpperCase(java.util.Locale.ENGLISH);
        }
        return new String(chars, 0, count).toUpperCase(java.util.Locale.ENGLISH);
    }
}
