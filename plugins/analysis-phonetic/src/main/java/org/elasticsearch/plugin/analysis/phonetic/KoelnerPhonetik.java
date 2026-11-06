/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.phonetic;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.StringEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * K&ouml;lner Phonetik
 *
 * H.J. Postel, Die K&ouml;lner Phonetik. Ein Verfahren zu Identifizierung
 * von Personennamen auf der Grundlage der Gestaltanalyse. IBM-Nachrichten 19 (1969), 925-931
 *
 * Algorithmus aus der Matching Toolbox von Rainer Schnell
 * Java-Programmierung von J&ouml;rg Reiher
 *
 * mit &Auml;nderungen von Jörg Prante
 *
 */
public class KoelnerPhonetik implements StringEncoder {

    private static final String[] POSTEL_VARIATIONS_PATTERNS = { "AUN", "OWN", "RB", "RW", "WSK", "RSK" };
    private static final String[] POSTEL_VARIATIONS_REPLACEMENTS = { "OWN", "AUN", "RW", "RB", "RSK", "WSK" };

    // Whitespace, control and punctuation characters split a token into the segments generateParts() combines.
    private static final Pattern SEGMENT_SEPARATOR = Pattern.compile("[\\p{Z}\\p{C}\\p{P}]");
    private static final Pattern SEPARATORS_ONLY = Pattern.compile("[\\p{Z}\\p{C}\\p{P}]*");

    /**
     * Default for {@code maxVariations}. 256 fits 8 pattern matches in one word, or a 22-segment token (253 parts)
     * with no matches, which is well beyond what real names need while keeping the worst-case cost of one token small.
     */
    static final int DEFAULT_MAX_VARIATIONS = 256;

    /**
     * Largest {@code maxVariations} accepted. generateParts() passes {@code maxVariations + 1} as the split() limit,
     * and overflowing that to a negative limit would make split() unbounded again.
     */
    static final int MAX_VARIATIONS_UPPER_BOUND = Integer.MAX_VALUE - 1;

    /**
     * Limit on the number of variations one {@code encode()} call may produce, shared across every part that
     * {@code partition()} generates. Each pattern match in a part doubles its variations, and a token with
     * n separator-delimited segments yields n(n+1)/2 parts, so without this limit certain tokens (e.g. "AUN" repeated,
     * or many hyphenated segments) can exhaust the heap. The limit removes the possibility of exponential heap growth.
     */
    private final int maxVariations;

    private Pattern[] variationsPatterns;
    private boolean primary = false;
    private final Set<Character> csz = new HashSet<>(Arrays.asList('C', 'S', 'Z'));
    private final Set<Character> ckq = new HashSet<>(Arrays.asList('C', 'K', 'Q'));
    private final Set<Character> aouhkxq = new HashSet<>(Arrays.asList('A', 'O', 'U', 'H', 'K', 'X', 'Q'));
    private final Set<Character> ahkloqrux = new HashSet<>(Arrays.asList('A', 'H', 'K', 'L', 'O', 'Q', 'R', 'U', 'X'));

    /**
     * Constructor for  Kölner Phonetik
     */
    public KoelnerPhonetik() {
        this(DEFAULT_MAX_VARIATIONS);
    }

    /**
     * @param maxVariations limit on the variations generated for one token, see {@code maxVariations}; between 1 and
     *                      {@link #MAX_VARIATIONS_UPPER_BOUND}
     */
    @SuppressWarnings("this-escape")
    public KoelnerPhonetik(int maxVariations) {
        if (maxVariations < 1 || maxVariations > MAX_VARIATIONS_UPPER_BOUND) {
            throw new IllegalArgumentException(
                "maxVariations must be between 1 and " + MAX_VARIATIONS_UPPER_BOUND + ", got [" + maxVariations + "]"
            );
        }
        this.maxVariations = maxVariations;
        init();
    }

    public KoelnerPhonetik(boolean useOnlyPrimaryCode) {
        this();
        this.primary = useOnlyPrimaryCode;
    }

    /**
     * Get variation patterns
     *
     * @return string array of variations
     */
    protected String[] getPatterns() {
        return POSTEL_VARIATIONS_PATTERNS;
    }

    protected String[] getReplacements() {
        return POSTEL_VARIATIONS_REPLACEMENTS;
    }

    protected char getCode() {
        return '0';
    }

    public double getRelativeValue(Object o1, Object o2) {
        String[] kopho1 = code(expandUmlauts(o1.toString().toUpperCase(Locale.GERMANY)));
        String[] kopho2 = code(expandUmlauts(o2.toString().toUpperCase(Locale.GERMANY)));
        for (int i = 0; i < kopho1.length; i++) {
            for (int ii = 0; ii < kopho2.length; ii++) {
                if (kopho1[i].equals(kopho2[ii])) {
                    return 1;
                }
            }
        }
        return 0;
    }

    @Override
    public Object encode(Object str) throws EncoderException {
        return encode((String) str);
    }

    @Override
    public String encode(String str) throws EncoderException {
        if (str == null) return null;
        String[] s = code(str.toString());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length; i++) {
            sb.append(s[i]);
            if (i < s.length - 1) {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    private void init() {
        this.variationsPatterns = new Pattern[getPatterns().length];
        for (int i = 0; i < getPatterns().length; i++) {
            this.variationsPatterns[i] = Pattern.compile(getPatterns()[i]);
        }
    }

    private String[] code(String str) {
        List<String> parts = partition(str);
        String[] codes = new String[parts.size()];
        int i = 0;
        for (String s : parts) {
            codes[i++] = substitute(s);
        }
        return codes;
    }

    private List<String> partition(String str) {
        List<String> parts = generateParts(str);
        List<String> variations = new ArrayList<>();
        // Share a single budget across all parts so the total variations for the whole token stay bounded,
        // rather than allowing each part to independently produce up to maxVariations.
        int remainingBudget = maxVariations;
        for (int i = 0; i < parts.size() && remainingBudget > 0; i++) {
            List<String> variation = getVariations(parts.get(i), remainingBudget);
            variations.addAll(variation);
            remainingBudget -= variation.size();
        }
        return variations;
    }

    // A token with n punctuation/whitespace-separated segments yields every contiguous run of segments as
    // its own part, i.e. n(n+1)/2 parts. Stop generating parts once the shared maxVariations budget is
    // reached so that count can't grow unbounded either, independent of the downstream variations budget in
    // partition(); otherwise a token with many segments would still materialize a huge intermediate parts
    // list even though the final variations count stays capped.
    List<String> generateParts(String str) {
        List<String> parts = new ArrayList<>();
        parts.add(str.replaceAll("[^\\p{L}\\p{N}]", ""));
        if (primary == false) {
            // Cap String.split() at maxVariations + 1 segments instead of splitting the whole token
            // unbounded: without a limit, split() allocates a String and array/list slot per segment before
            // the cap above ever gets a chance to apply, so a punctuation-heavy token with many segments
            // would still force an allocation proportional to its segment count. The "+ 1" is a single extra
            // slot used only to detect whether more segments exist beyond what we keep.
            String[] rawParts = SEGMENT_SEPARATOR.split(str, maxVariations + 1);
            // A positive split() limit, unlike the unlimited split() it replaces, does not drop trailing
            // empty strings, and a long enough run of trailing separators lands entirely inside the final
            // slot as one non-empty separator-only string (e.g. "----"). Trim any trailing slot that is
            // wholly separator characters (including empty ones, matched by "*" as zero occurrences), not
            // just empty ones, so trailing separators have no effect regardless of how many there are.
            int effectiveLength = rawParts.length;
            while (effectiveLength > 0 && SEPARATORS_ONLY.matcher(rawParts[effectiveLength - 1]).matches()) {
                effectiveLength--;
            }
            boolean moreSegmentsExist = effectiveLength > maxVariations;
            int keptCount = moreSegmentsExist ? maxVariations : effectiveLength;
            List<String> tmpParts = new ArrayList<>(Arrays.asList(rawParts).subList(0, keptCount));
            // If more segments exist beyond what we kept, use a numberOfParts the loop below can never reach,
            // since the "skip the final full-string concatenation" check it's used for only applies to the
            // token's true last segment, which isn't in our truncated batch.
            int numberOfParts = moreSegmentsExist ? Integer.MAX_VALUE : tmpParts.size();
            while (!tmpParts.isEmpty() && parts.size() < maxVariations) {
                StringBuilder part = new StringBuilder();
                for (int i = 0; i < tmpParts.size() && parts.size() < maxVariations; i++) {
                    part.append(tmpParts.get(i));
                    if ((i + 1 == numberOfParts) == false) {
                        parts.add(part.toString());
                    }
                }
                tmpParts.remove(0);
            }
        }
        return parts;
    }

    private List<String> getVariations(String str, int budget) {
        int position = 0;
        List<String> variations = new ArrayList<>();
        variations.add("");
        while (position < str.length()) {
            int i = 0;
            int substPos = -1;
            while (substPos < position && i < getPatterns().length) {
                Matcher m = variationsPatterns[i].matcher(str);
                while (substPos < position && m.find()) {
                    substPos = m.start();
                }
                i++;
            }
            if (substPos >= position) {
                i--;
                String prevPart = str.substring(position, substPos);

                // Fix the pre-branch size so the loop below only rewrites the existing entries, not the
                // replacement variants just appended to varNew. Add only as many replacement branches as fit
                // under the budget, rather than always doubling, so a budget that isn't a power of two
                // (e.g. left over from an earlier part) can't be overshot.
                int sizeBeforeBranching = variations.size();
                if (sizeBeforeBranching >= budget) {
                    String suffix = str.substring(position);
                    for (int ii = 0; ii < sizeBeforeBranching; ii++) {
                        variations.set(ii, variations.get(ii) + suffix);
                    }
                    break;
                }
                int branchesToAdd = Math.min(sizeBeforeBranching, budget - sizeBeforeBranching);
                List<String> varNew = new ArrayList<>(branchesToAdd);
                for (int ii = 0; ii < sizeBeforeBranching; ii++) {
                    if (ii < branchesToAdd) {
                        varNew.add(variations.get(ii).concat(prevPart + getReplacements()[i]));
                    }
                    variations.set(ii, variations.get(ii) + prevPart + getPatterns()[i]);
                }
                variations.addAll(varNew);
                position = substPos + getPatterns()[i].length();
            } else {
                for (int ii = 0; ii < variations.size(); ii++) {
                    variations.set(ii, variations.get(ii) + str.substring(position, str.length()));
                }
                position = str.length();
            }
        }
        return variations;
    }

    private String substitute(String str) {
        String s = expandUmlauts(str.toUpperCase(Locale.GERMAN));
        s = removeSequences(s);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char current = s.charAt(i);
            char next = i + 1 < s.length() ? s.charAt(i + 1) : '_';
            char prev = i > 0 ? s.charAt(i - 1) : '_';
            switch (current) {
                case 'A':
                case 'E':
                case 'I':
                case 'J':
                case 'Y':
                case 'O':
                case 'U':
                    if (i == 0 || ((i == 1) && prev == 'H')) {
                        sb.append(getCode());
                    }
                    break;
                case 'P':
                    sb.append(next == 'H' ? "33" : '1');
                    break;
                case 'B':
                    sb.append('1');
                    break;
                case 'D':
                case 'T':
                    sb.append(csz.contains(next) ? '8' : '2');
                    break;
                case 'F':
                case 'V':
                case 'W':
                    sb.append('3');
                    break;
                case 'G':
                case 'K':
                case 'Q':
                    sb.append('4');
                    break;
                case 'C':
                    if (i == 0) {
                        sb.append(ahkloqrux.contains(next) ? '4' : '8');
                    } else {
                        sb.append(aouhkxq.contains(next) ? '4' : '8');
                    }
                    if (sb.length() >= 2 && sb.charAt(sb.length() - 2) == '8') {
                        sb.setCharAt(sb.length() - 1, '8');
                    }
                    break;
                case 'X':
                    sb.append(i < 1 || ckq.contains(prev) == false ? "48" : '8');
                    break;
                case 'L':
                    sb.append('5');
                    break;
                case 'M':
                case 'N':
                    sb.append('6');
                    break;
                case 'R':
                    sb.append('7');
                    break;
                case 'S':
                case 'Z':
                    sb.append('8');
                    break;
                case 'H':
                    break;
            }
        }
        s = sb.toString();
        s = removeSequences(s);
        return s;
    }

    private static String expandUmlauts(String str) {
        return str.replace("\u00C4", "AE").replace("\u00D6", "OE").replace("\u00DC", "UE");
    }

    private static String removeSequences(String str) {
        if (str == null || str.length() == 0) {
            return "";
        }
        int i = 0, j = 0;
        StringBuilder sb = new StringBuilder().append(str.charAt(i++));
        char c;
        while (i < str.length()) {
            c = str.charAt(i);
            if (c != sb.charAt(j)) {
                sb.append(c);
                j++;
            }
            i++;
        }
        return sb.toString();
    }
}
