/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;

import static java.util.Map.entry;

/**
 * The Unicode data the parser needs, from the JDK. It stands in for RE2/J's {@code Unicode} and {@code UnicodeTables},
 * which are generated from Unicode 6.0; Go 1.26, which Prometheus is built with, is on Unicode 15.0, and the JDK on a
 * version at least as new.
 */
final class Unicode {

    static final int MAX_RUNE = Character.MAX_CODE_POINT;

    /** Every code point that has another case, sorted. */
    private static final int[] FOLDING;
    /** {@code ORBITS[i]} is the orbit of {@code FOLDING[i]}, itself included, sorted. */
    private static final int[][] ORBITS;

    static {
        Map<Integer, List<Integer>> orbits = new HashMap<>();
        for (int c = 0; c <= MAX_RUNE; c++) {
            orbits.computeIfAbsent(foldKey(c), k -> new ArrayList<>()).add(c);
        }
        Map<Integer, int[]> orbitOf = new HashMap<>();
        for (List<Integer> orbit : orbits.values()) {
            if (orbit.size() > 1) {
                int[] members = orbit.stream().mapToInt(Integer::intValue).toArray();
                for (int c : members) {
                    orbitOf.put(c, members);
                }
            }
        }
        FOLDING = orbitOf.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        ORBITS = new int[FOLDING.length][];
        for (int i = 0; i < FOLDING.length; i++) {
            ORBITS[i] = orbitOf.get(FOLDING[i]);
        }
    }

    /** The smallest and largest code points that have another case. */
    static final int MIN_FOLD = FOLDING[0];
    static final int MAX_FOLD = FOLDING[FOLDING.length - 1];

    private Unicode() {}

    /** Two code points are in one orbit if they fold to the same key. */
    private static int foldKey(int c) {
        // Go doesn't fold the Turkish dotted and dotless i, which the JDK maps to i and I
        if (c == 0x130 || c == 0x131) {
            return c;
        }
        return Character.toLowerCase(Character.toUpperCase(c));
    }

    /**
     * As Go's {@code unicode.SimpleFold}: the smallest code point in {@code r}'s orbit that is larger than {@code r},
     * or the smallest in the orbit if there is none, or {@code r} itself if it has no other case.
     */
    static int simpleFold(int r) {
        int i = Arrays.binarySearch(FOLDING, r);
        if (i < 0) {
            return r;
        }
        int[] orbit = ORBITS[i];
        for (int c : orbit) {
            if (c > r) {
                return c;
            }
        }
        return orbit[0];
    }

    /** As Go's {@code unicode.IsPrint}: a letter, mark, number, punctuation, symbol or the ASCII space. */
    static boolean isPrint(int r) {
        if (r == ' ') {
            return true;
        }
        char category = TYPE_CATEGORIES.get(Character.getType(r)).charAt(0);
        return category == 'L' || category == 'M' || category == 'N' || category == 'P' || category == 'S';
    }

    /**
     * A class's code points as {@code {lo, hi, stride}} ranges, those in their case orbits, and whether the class is
     * the code points outside them.
     */
    record Table(int[][] ranges, int[][] fold, boolean inverted) {}

    private static final Map<String, Table> TABLES = new ConcurrentHashMap<>();

    /**
     * The class {@code \p{name}} names, looked up as Go 1.26 does: case-insensitively and ignoring spaces, underscores
     * and hyphens, among the general categories, their long aliases, and the scripts with a one-word name. Null if
     * there is none.
     */
    static Table table(String name) {
        String canonical = canonicalName(name);
        switch (canonical) {
            case "Any" -> {
                return TABLES.computeIfAbsent(canonical, k -> table(c -> true, false));
            }
            case "Ascii" -> {
                return TABLES.computeIfAbsent(canonical, k -> table(c -> c <= 0x7F, false));
            }
            case "Assigned" -> {
                return TABLES.computeIfAbsent(canonical, k -> table(category("Cn"), true));
            }
            case "Lc" -> {
                return TABLES.computeIfAbsent(canonical, k -> table(category("LC"), false));
            }
            default -> {
                // a category, a script or a category alias, below
            }
        }
        if (CATEGORIES.contains(canonical)) {
            return TABLES.computeIfAbsent(canonical, k -> table(category(canonical), false));
        }
        if (SCRIPTS.contains(canonical)) {
            Character.UnicodeScript script = Character.UnicodeScript.valueOf(canonical.toUpperCase(Locale.ROOT));
            return TABLES.computeIfAbsent(canonical, k -> table(c -> Character.UnicodeScript.of(c) == script, false));
        }
        String alias = CATEGORY_ALIASES.get(canonical);
        return alias == null ? null : TABLES.computeIfAbsent(alias, k -> table(category(alias), false));
    }

    /** Go's canonical name: an upper case first letter, the rest lower case, without spaces, underscores or hyphens. */
    private static String canonicalName(String name) {
        StringBuilder canonical = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == ' ' || c == '_' || c == '-') {
                continue;
            }
            if (canonical.isEmpty()) {
                canonical.append(c >= 'a' && c <= 'z' ? (char) (c - 'a' + 'A') : c);
            } else {
                canonical.append(c >= 'A' && c <= 'Z' ? (char) (c - 'A' + 'a') : c);
            }
        }
        return canonical.toString();
    }

    /** A one-letter category is all of its two-letter ones, and LC is Lu, Ll and Lt. */
    private static IntPredicate category(String name) {
        return c -> {
            String category = TYPE_CATEGORIES.get(Character.getType(c));
            if (name.length() == 1) {
                return category.charAt(0) == name.charAt(0);
            }
            if (name.equals("LC")) {
                return category.equals("Lu") || category.equals("Ll") || category.equals("Lt");
            }
            return category.equals(name);
        };
    }

    private static Table table(IntPredicate member, boolean inverted) {
        CharClass ranges = new CharClass();
        int start = -1;
        for (int c = 0; c <= MAX_RUNE + 1; c++) {
            boolean in = c <= MAX_RUNE && member.test(c);
            if (in && start < 0) {
                start = c;
            } else if (in == false && start >= 0) {
                ranges.appendRange(start, c - 1);
                start = -1;
            }
        }
        int[] pairs = ranges.cleanClass().toArray();
        int[] folded = new CharClass().appendFoldedClass(pairs).cleanClass().toArray();
        return new Table(strided(pairs), strided(folded), inverted);
    }

    private static int[][] strided(int[] pairs) {
        int[][] ranges = new int[pairs.length / 2][];
        for (int i = 0; i < pairs.length; i += 2) {
            ranges[i / 2] = new int[] { pairs[i], pairs[i + 1], 1 };
        }
        return ranges;
    }

    private static final Set<String> CATEGORIES = Set.of(
        "C",
        "Cc",
        "Cf",
        "Cn",
        "Co",
        "Cs",
        "L",
        "Ll",
        "Lm",
        "Lo",
        "Lt",
        "Lu",
        "M",
        "Mc",
        "Me",
        "Mn",
        "N",
        "Nd",
        "Nl",
        "No",
        "P",
        "Pc",
        "Pd",
        "Pe",
        "Pf",
        "Pi",
        "Po",
        "Ps",
        "S",
        "Sc",
        "Sk",
        "Sm",
        "So",
        "Z",
        "Zl",
        "Zp",
        "Zs"
    );

    private static final Map<String, String> CATEGORY_ALIASES = Map.ofEntries(
        entry("Casedletter", "LC"),
        entry("Closepunctuation", "Pe"),
        entry("Cntrl", "Cc"),
        entry("Combiningmark", "M"),
        entry("Connectorpunctuation", "Pc"),
        entry("Control", "Cc"),
        entry("Currencysymbol", "Sc"),
        entry("Dashpunctuation", "Pd"),
        entry("Decimalnumber", "Nd"),
        entry("Digit", "Nd"),
        entry("Enclosingmark", "Me"),
        entry("Finalpunctuation", "Pf"),
        entry("Format", "Cf"),
        entry("Initialpunctuation", "Pi"),
        entry("Letter", "L"),
        entry("Letternumber", "Nl"),
        entry("Lineseparator", "Zl"),
        entry("Lowercaseletter", "Ll"),
        entry("Mark", "M"),
        entry("Mathsymbol", "Sm"),
        entry("Modifierletter", "Lm"),
        entry("Modifiersymbol", "Sk"),
        entry("Nonspacingmark", "Mn"),
        entry("Number", "N"),
        entry("Openpunctuation", "Ps"),
        entry("Other", "C"),
        entry("Otherletter", "Lo"),
        entry("Othernumber", "No"),
        entry("Otherpunctuation", "Po"),
        entry("Othersymbol", "So"),
        entry("Paragraphseparator", "Zp"),
        entry("Privateuse", "Co"),
        entry("Punct", "P"),
        entry("Punctuation", "P"),
        entry("Separator", "Z"),
        entry("Spaceseparator", "Zs"),
        entry("Spacingmark", "Mc"),
        entry("Surrogate", "Cs"),
        entry("Symbol", "S"),
        entry("Titlecaseletter", "Lt"),
        entry("Unassigned", "Cn"),
        entry("Uppercaseletter", "Lu")
    );

    /** Go's scripts reachable through its name lookup, which misses multi-word ones like {@code Old_Italic}. */
    private static final Set<String> SCRIPTS = Set.of(
        "Adlam",
        "Ahom",
        "Arabic",
        "Armenian",
        "Avestan",
        "Balinese",
        "Bamum",
        "Batak",
        "Bengali",
        "Bhaiksuki",
        "Bopomofo",
        "Brahmi",
        "Braille",
        "Buginese",
        "Buhid",
        "Carian",
        "Chakma",
        "Cham",
        "Cherokee",
        "Chorasmian",
        "Common",
        "Coptic",
        "Cuneiform",
        "Cypriot",
        "Cyrillic",
        "Deseret",
        "Devanagari",
        "Dogra",
        "Duployan",
        "Elbasan",
        "Elymaic",
        "Ethiopic",
        "Georgian",
        "Glagolitic",
        "Gothic",
        "Grantha",
        "Greek",
        "Gujarati",
        "Gurmukhi",
        "Han",
        "Hangul",
        "Hanunoo",
        "Hatran",
        "Hebrew",
        "Hiragana",
        "Inherited",
        "Javanese",
        "Kaithi",
        "Kannada",
        "Katakana",
        "Kawi",
        "Kharoshthi",
        "Khmer",
        "Khojki",
        "Khudawadi",
        "Lao",
        "Latin",
        "Lepcha",
        "Limbu",
        "Lisu",
        "Lycian",
        "Lydian",
        "Mahajani",
        "Makasar",
        "Malayalam",
        "Mandaic",
        "Manichaean",
        "Marchen",
        "Medefaidrin",
        "Miao",
        "Modi",
        "Mongolian",
        "Mro",
        "Multani",
        "Myanmar",
        "Nabataean",
        "Nandinagari",
        "Newa",
        "Nko",
        "Nushu",
        "Ogham",
        "Oriya",
        "Osage",
        "Osmanya",
        "Palmyrene",
        "Phoenician",
        "Rejang",
        "Runic",
        "Samaritan",
        "Saurashtra",
        "Sharada",
        "Shavian",
        "Siddham",
        "Sinhala",
        "Sogdian",
        "Soyombo",
        "Sundanese",
        "Syriac",
        "Tagalog",
        "Tagbanwa",
        "Takri",
        "Tamil",
        "Tangsa",
        "Tangut",
        "Telugu",
        "Thaana",
        "Thai",
        "Tibetan",
        "Tifinagh",
        "Tirhuta",
        "Toto",
        "Ugaritic",
        "Vai",
        "Vithkuqi",
        "Wancho",
        "Yezidi",
        "Yi"
    );

    /** The two-letter general category of each {@link Character#getType} value. */
    private static final Map<Integer, String> TYPE_CATEGORIES = Map.ofEntries(
        entry((int) Character.CONTROL, "Cc"),
        entry((int) Character.FORMAT, "Cf"),
        entry((int) Character.UNASSIGNED, "Cn"),
        entry((int) Character.PRIVATE_USE, "Co"),
        entry((int) Character.SURROGATE, "Cs"),
        entry((int) Character.LOWERCASE_LETTER, "Ll"),
        entry((int) Character.MODIFIER_LETTER, "Lm"),
        entry((int) Character.OTHER_LETTER, "Lo"),
        entry((int) Character.TITLECASE_LETTER, "Lt"),
        entry((int) Character.UPPERCASE_LETTER, "Lu"),
        entry((int) Character.COMBINING_SPACING_MARK, "Mc"),
        entry((int) Character.ENCLOSING_MARK, "Me"),
        entry((int) Character.NON_SPACING_MARK, "Mn"),
        entry((int) Character.DECIMAL_DIGIT_NUMBER, "Nd"),
        entry((int) Character.LETTER_NUMBER, "Nl"),
        entry((int) Character.OTHER_NUMBER, "No"),
        entry((int) Character.CONNECTOR_PUNCTUATION, "Pc"),
        entry((int) Character.DASH_PUNCTUATION, "Pd"),
        entry((int) Character.END_PUNCTUATION, "Pe"),
        entry((int) Character.FINAL_QUOTE_PUNCTUATION, "Pf"),
        entry((int) Character.INITIAL_QUOTE_PUNCTUATION, "Pi"),
        entry((int) Character.OTHER_PUNCTUATION, "Po"),
        entry((int) Character.START_PUNCTUATION, "Ps"),
        entry((int) Character.CURRENCY_SYMBOL, "Sc"),
        entry((int) Character.MODIFIER_SYMBOL, "Sk"),
        entry((int) Character.MATH_SYMBOL, "Sm"),
        entry((int) Character.OTHER_SYMBOL, "So"),
        entry((int) Character.LINE_SEPARATOR, "Zl"),
        entry((int) Character.PARAGRAPH_SEPARATOR, "Zp"),
        entry((int) Character.SPACE_SEPARATOR, "Zs")
    );
}
