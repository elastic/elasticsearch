/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        String primaryForm = str;
        List<String> parts = new ArrayList<>();
        parts.add(primaryForm.replaceAll("[^\\p{L}\\p{N}]", ""));
        if (primary == false) {
            List<String> tmpParts = new ArrayList<>(Arrays.asList(str.split("[\\p{Z}\\p{C}\\p{P}]")));
            int numberOfParts = tmpParts.size();
            while (tmpParts.size() > 0) {
                StringBuilder part = new StringBuilder();
                for (int i = 0; i < tmpParts.size(); i++) {
                    part.append(tmpParts.get(i));
                    if ((i + 1 == numberOfParts) == false) {
                        parts.add(part.toString());
                    }
                }
                tmpParts.remove(0);
            }
        }
        List<String> variations = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            List<String> variation = getVariations(parts.get(i));
            if (variation != null) {
                variations.addAll(variation);
            }
        }
        return variations;
    }

    private List<String> getVariations(String str) {
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
                List<String> varNew = new ArrayList<>();
                String prevPart = str.substring(position, substPos);
                for (int ii = 0; ii < variations.size(); ii++) {
                    String tmp = variations.get(ii);
                    varNew.add(tmp.concat(prevPart + getReplacements()[i]));
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

    private String expandUmlauts(String str) {
        return str.replaceAll("\u00C4", "AE").replaceAll("\u00D6", "OE").replaceAll("\u00DC", "UE");
    }

    private String removeSequences(String str) {
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
