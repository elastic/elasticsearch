/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.UnicodeTables;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Looks Unicode classes up by the names {@code \p} takes, as Go 1.26 does, in the tables generated for the fork of
 * RE2/J in the {@code re2} package: case-insensitively and ignoring spaces, underscores and hyphens, among the general
 * categories, their long aliases, and the scripts with a one-word name.
 */
public final class Re2UnicodeNames {

    /** A class's code points as {@code {lo, hi, stride}} ranges, those that fold to them, and whether it is the rest. */
    public record Table(int[][] ranges, int[][] fold, boolean inverted) {}

    private static final int[][] ANY = { { 0, Character.MAX_CODE_POINT, 1 } };
    private static final int[][] ASCII = { { 0, 0x7F, 1 } };
    /** The code points outside ASCII that fold into it, the long s and the Kelvin sign, as Go has them. */
    private static final int[][] ASCII_FOLD = { { 0x17F, 0x17F, 1 }, { 0x212A, 0x212A, 1 } };

    private Re2UnicodeNames() {}

    /** The class {@code \p{name}} names; null if there is none. */
    public static Table table(String name) {
        String canonical = canonicalName(name);
        switch (canonical) {
            case "Any" -> {
                return new Table(ANY, ANY, false);
            }
            case "Ascii" -> {
                return new Table(ASCII, ASCII_FOLD, false);
            }
            case "Assigned" -> {
                int[][] unassigned = UnicodeTables.CATEGORIES.get("Cn");
                return new Table(unassigned, unassigned, true);
            }
            case "Lc" -> {
                return category("LC");
            }
            default -> {
                // a category, a script or a category alias, below
            }
        }
        Table category = category(canonical);
        if (category != null) {
            return category;
        }
        int[][] script = UnicodeTables.SCRIPTS.get(canonical);
        if (script != null) {
            return new Table(script, UnicodeTables.FOLD_SCRIPT.get(canonical), false);
        }
        String alias = CATEGORY_ALIASES.get(canonical);
        return alias == null ? null : category(alias);
    }

    private static Table category(String name) {
        int[][] category = UnicodeTables.CATEGORIES.get(name);
        return category == null ? null : new Table(category, UnicodeTables.FOLD_CATEGORIES.get(name), false);
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

    /** The other names of the categories, Go's {@code CategoryAliases}, canonical. */
    private static final Map<String, String> CATEGORY_ALIASES = UnicodeTables.CATEGORY_ALIASES.entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(alias -> canonicalName(alias.getKey()), Map.Entry::getValue));
}
