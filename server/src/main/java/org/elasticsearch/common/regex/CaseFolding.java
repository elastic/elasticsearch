/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.regex;

class CaseFolding {

    /**
     * This attempts to find the lowest possible state this codepoint can be in based on the Unicode case folding spec:
     * https://www.unicode.org/Public/16.0.0/ucd/CaseFolding.txt
     *
     * <p>this is intended to be used in matching and the expectation that both this string and the matched string
     * utilize this function to normalize the forms to a "ground" state.  There is no guarantee that the codepoint is actually
     * the least value in set of the codepoints that would be matched but in general this will be true
     *
     * @param codepoint to fold to the lowest possible state that would be a match in a case-insensitive match
     * @return
     */
    static int lowestCodePoint(int codepoint) {
        int[] alts = lookupAlternates(codepoint);
        if (alts != null) {
            int lowest = codepoint;
            for (int i = 0; i < alts.length; i++) {
                if (alts[i] < lowest) {
                    lowest = alts[i];
                }
            }
            return lowest;
        } else {
            // generally this is true and sufficient for most needs to normalize the codepoint to a common form
            return Character.toUpperCase(codepoint);
        }
    }

    /**
     * Generates the set of codepoints which represent the given codepoint that are case-insensitive
     * matches within the Unicode table, which may not always be intuitive for instance Σ, σ, ς do all
     * fold to one another and so would match one another
     *
     * <p>Known special cases derived from generating mappings using a combination of the Unicode
     * 16.0.0 spec: https://www.unicode.org/Public/16.0.0/ucd/CaseFolding.txt and
     * https://www.unicode.org/Public/UCD/latest/ucd/UnicodeData.txt these are only the alternate
     * mappings for each codepoint that are not supported by a transform using Character.toUpperCase
     * or Character.toLowerCase
     *
     * @param codepoint the codepoint for the character to case fold
     * @return an array of characters as codepoints that should match the given codepoint in a
     *     case-insensitive context or null if no alternates exist this does not include the given
     *     codepoint itself
     */
    static int[] lookupAlternates(int codepoint) {

        int[] alts = switch (codepoint) {
            case 0x00049 -> // I [LATIN CAPITAL LETTER I]
                new int[] {
                    0x00130, // İ [LATIN CAPITAL LETTER I WITH DOT ABOVE]
                    0x00131, // ı [LATIN SMALL LETTER DOTLESS I]
                    0x00069, // i [LATIN SMALL LETTER I]
                };
            case 0x0004B -> // K [LATIN CAPITAL LETTER K]
                new int[] {
                    0x0212A, // K [KELVIN SIGN]
                    0x0006B, // k [LATIN SMALL LETTER K]
                };
            case 0x00053 -> // S [LATIN CAPITAL LETTER S]
                new int[] {
                    0x00073, // s [LATIN SMALL LETTER S]
                    0x0017F, // ſ [LATIN SMALL LETTER LONG S]
                };
            case 0x00069 -> // i [LATIN SMALL LETTER I]
                new int[] {
                    0x00130, // İ [LATIN CAPITAL LETTER I WITH DOT ABOVE]
                    0x00131, // ı [LATIN SMALL LETTER DOTLESS I]
                    0x00049, // I [LATIN CAPITAL LETTER I]
                };
            case 0x0006B -> // k [LATIN SMALL LETTER K]
                new int[] {
                    0x0212A, // K [KELVIN SIGN]
                    0x0004B, // K [LATIN CAPITAL LETTER K]
                };
            case 0x00073 -> // s [LATIN SMALL LETTER S]
                new int[] {
                    0x00053, // S [LATIN CAPITAL LETTER S]
                    0x0017F, // ſ [LATIN SMALL LETTER LONG S]
                };
            case 0x000B5 -> // µ [MICRO SIGN]
                new int[] {
                    0x0039C, // Μ [GREEK CAPITAL LETTER MU]
                    0x003BC, // μ [GREEK SMALL LETTER MU]
                };
            case 0x000C5 -> // Å [LATIN CAPITAL LETTER A WITH RING ABOVE]
                new int[] {
                    0x000E5, // å [LATIN SMALL LETTER A WITH RING ABOVE]
                    0x0212B, // Å [ANGSTROM SIGN]
                };
            case 0x000DF -> // ß [LATIN SMALL LETTER SHARP S]
                new int[] { 0x01E9E, // ẞ [LATIN CAPITAL LETTER SHARP S]
                };
            case 0x000E5 -> // å [LATIN SMALL LETTER A WITH RING ABOVE]
                new int[] {
                    0x000C5, // Å [LATIN CAPITAL LETTER A WITH RING ABOVE]
                    0x0212B, // Å [ANGSTROM SIGN]
                };
            case 0x02126 -> // Ω [OHM SIGN]
                new int[] {
                    0x003A9, // Ω [GREEK CAPITAL LETTER OMEGA]
                    0x003C9, // ω [GREEK SMALL LETTER OMEGA]
                };
            case 0x0212A -> // K [KELVIN SIGN]
                new int[] {
                    0x0004B, // K [LATIN CAPITAL LETTER K]
                    0x0006B, // k [LATIN SMALL LETTER K]
                };
            case 0x0212B -> // Å [ANGSTROM SIGN]
                new int[] {
                    0x000C5, // Å [LATIN CAPITAL LETTER A WITH RING ABOVE]
                    0x000E5, // å [LATIN SMALL LETTER A WITH RING ABOVE]
                };
            case 0x00130 -> // İ [LATIN CAPITAL LETTER I WITH DOT ABOVE]
                new int[] {
                    0x00131, // ı [LATIN SMALL LETTER DOTLESS I]
                    0x00049, // I [LATIN CAPITAL LETTER I]
                    0x00069, // i [LATIN SMALL LETTER I]
                };
            case 0x00131 -> // ı [LATIN SMALL LETTER DOTLESS I]
                new int[] {
                    0x00130, // İ [LATIN CAPITAL LETTER I WITH DOT ABOVE]
                    0x00069, // i [LATIN SMALL LETTER I]
                    0x00049, // I [LATIN CAPITAL LETTER I]
                };
            case 0x0017F -> // ſ [LATIN SMALL LETTER LONG S]
                new int[] {
                    0x00053, // S [LATIN CAPITAL LETTER S]
                    0x00073, // s [LATIN SMALL LETTER S]
                };
            case 0x0019B -> // ƛ [LATIN SMALL LETTER LAMBDA WITH STROKE]
                new int[] { 0x0A7DC, // Ƛ [LATIN CAPITAL LETTER LAMBDA WITH STROKE]
                };
            case 0x001C4 -> // Ǆ [LATIN CAPITAL LETTER DZ WITH CARON]
                new int[] {
                    0x001C5, // ǅ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
                    0x001C6, // ǆ [LATIN SMALL LETTER DZ WITH CARON]
                };
            case 0x001C5 -> // ǅ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
                new int[] {
                    0x001C4, // Ǆ [LATIN CAPITAL LETTER DZ WITH CARON]
                    0x001C6, // ǆ [LATIN SMALL LETTER DZ WITH CARON]
                };
            case 0x001C6 -> // ǆ [LATIN SMALL LETTER DZ WITH CARON]
                new int[] {
                    0x001C4, // Ǆ [LATIN CAPITAL LETTER DZ WITH CARON]
                    0x001C5, // ǅ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
                };
            case 0x001C7 -> // Ǉ [LATIN CAPITAL LETTER LJ]
                new int[] {
                    0x001C8, // ǈ [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
                    0x001C9, // ǉ [LATIN SMALL LETTER LJ]
                };
            case 0x001C8 -> // ǈ [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
                new int[] {
                    0x001C7, // Ǉ [LATIN CAPITAL LETTER LJ]
                    0x001C9, // ǉ [LATIN SMALL LETTER LJ]
                };
            case 0x001C9 -> // ǉ [LATIN SMALL LETTER LJ]
                new int[] {
                    0x001C7, // Ǉ [LATIN CAPITAL LETTER LJ]
                    0x001C8, // ǈ [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
                };
            case 0x001CA -> // Ǌ [LATIN CAPITAL LETTER NJ]
                new int[] {
                    0x001CB, // ǋ [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
                    0x001CC, // ǌ [LATIN SMALL LETTER NJ]
                };
            case 0x001CB -> // ǋ [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
                new int[] {
                    0x001CA, // Ǌ [LATIN CAPITAL LETTER NJ]
                    0x001CC, // ǌ [LATIN SMALL LETTER NJ]
                };
            case 0x001CC -> // ǌ [LATIN SMALL LETTER NJ]
                new int[] {
                    0x001CA, // Ǌ [LATIN CAPITAL LETTER NJ]
                    0x001CB, // ǋ [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
                };
            case 0x001F1 -> // Ǳ [LATIN CAPITAL LETTER DZ]
                new int[] {
                    0x001F2, // ǲ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
                    0x001F3, // ǳ [LATIN SMALL LETTER DZ]
                };
            case 0x001F2 -> // ǲ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
                new int[] {
                    0x001F1, // Ǳ [LATIN CAPITAL LETTER DZ]
                    0x001F3, // ǳ [LATIN SMALL LETTER DZ]
                };
            case 0x001F3 -> // ǳ [LATIN SMALL LETTER DZ]
                new int[] {
                    0x001F1, // Ǳ [LATIN CAPITAL LETTER DZ]
                    0x001F2, // ǲ [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
                };
            case 0x00264 -> // ɤ [LATIN SMALL LETTER RAMS HORN]
                new int[] { 0x0A7CB, // Ɤ [LATIN CAPITAL LETTER RAMS HORN]
                };
            case 0x00345 -> // ͅ [COMBINING GREEK YPOGEGRAMMENI]
                new int[] {
                    0x00399, // Ι [GREEK CAPITAL LETTER IOTA]
                    0x003B9, // ι [GREEK SMALL LETTER IOTA]
                    0x01FBE, // ι [GREEK PROSGEGRAMMENI]
                };
            case 0x00390 -> // ΐ [GREEK SMALL LETTER IOTA WITH DIALYTIKA AND TONOS]
                new int[] { 0x01FD3, // ΐ [GREEK SMALL LETTER IOTA WITH DIALYTIKA AND OXIA]
                };
            case 0x00392 -> // Β [GREEK CAPITAL LETTER BETA]
                new int[] {
                    0x003D0, // ϐ [GREEK BETA SYMBOL]
                    0x003B2, // β [GREEK SMALL LETTER BETA]
                };
            case 0x00395 -> // Ε [GREEK CAPITAL LETTER EPSILON]
                new int[] {
                    0x003F5, // ϵ [GREEK LUNATE EPSILON SYMBOL]
                    0x003B5, // ε [GREEK SMALL LETTER EPSILON]
                };
            case 0x00398 -> // Θ [GREEK CAPITAL LETTER THETA]
                new int[] {
                    0x003D1, // ϑ [GREEK THETA SYMBOL]
                    0x003F4, // ϴ [GREEK CAPITAL THETA SYMBOL]
                    0x003B8, // θ [GREEK SMALL LETTER THETA]
                };
            case 0x00399 -> // Ι [GREEK CAPITAL LETTER IOTA]
                new int[] {
                    0x00345, // ͅ [COMBINING GREEK YPOGEGRAMMENI]
                    0x003B9, // ι [GREEK SMALL LETTER IOTA]
                    0x01FBE, // ι [GREEK PROSGEGRAMMENI]
                };
            case 0x0039A -> // Κ [GREEK CAPITAL LETTER KAPPA]
                new int[] {
                    0x003F0, // ϰ [GREEK KAPPA SYMBOL]
                    0x003BA, // κ [GREEK SMALL LETTER KAPPA]
                };
            case 0x0039C -> // Μ [GREEK CAPITAL LETTER MU]
                new int[] {
                    0x000B5, // µ [MICRO SIGN]
                    0x003BC, // μ [GREEK SMALL LETTER MU]
                };
            case 0x003A0 -> // Π [GREEK CAPITAL LETTER PI]
                new int[] {
                    0x003C0, // π [GREEK SMALL LETTER PI]
                    0x003D6, // ϖ [GREEK PI SYMBOL]
                };
            case 0x003A1 -> // Ρ [GREEK CAPITAL LETTER RHO]
                new int[] {
                    0x003F1, // ϱ [GREEK RHO SYMBOL]
                    0x003C1, // ρ [GREEK SMALL LETTER RHO]
                };
            case 0x003A3 -> // Σ [GREEK CAPITAL LETTER SIGMA]
                new int[] {
                    0x003C2, // ς [GREEK SMALL LETTER FINAL SIGMA]
                    0x003C3, // σ [GREEK SMALL LETTER SIGMA]
                };
            case 0x003A6 -> // Φ [GREEK CAPITAL LETTER PHI]
                new int[] {
                    0x003D5, // ϕ [GREEK PHI SYMBOL]
                    0x003C6, // φ [GREEK SMALL LETTER PHI]
                };
            case 0x003A9 -> // Ω [GREEK CAPITAL LETTER OMEGA]
                new int[] {
                    0x02126, // Ω [OHM SIGN]
                    0x003C9, // ω [GREEK SMALL LETTER OMEGA]
                };
            case 0x003B0 -> // ΰ [GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND TONOS]
                new int[] { 0x01FE3, // ΰ [GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND OXIA]
                };
            case 0x003B2 -> // β [GREEK SMALL LETTER BETA]
                new int[] {
                    0x003D0, // ϐ [GREEK BETA SYMBOL]
                    0x00392, // Β [GREEK CAPITAL LETTER BETA]
                };
            case 0x003B5 -> // ε [GREEK SMALL LETTER EPSILON]
                new int[] {
                    0x00395, // Ε [GREEK CAPITAL LETTER EPSILON]
                    0x003F5, // ϵ [GREEK LUNATE EPSILON SYMBOL]
                };
            case 0x003B8 -> // θ [GREEK SMALL LETTER THETA]
                new int[] {
                    0x003D1, // ϑ [GREEK THETA SYMBOL]
                    0x003F4, // ϴ [GREEK CAPITAL THETA SYMBOL]
                    0x00398, // Θ [GREEK CAPITAL LETTER THETA]
                };
            case 0x003B9 -> // ι [GREEK SMALL LETTER IOTA]
                new int[] {
                    0x00345, // ͅ [COMBINING GREEK YPOGEGRAMMENI]
                    0x00399, // Ι [GREEK CAPITAL LETTER IOTA]
                    0x01FBE, // ι [GREEK PROSGEGRAMMENI]
                };
            case 0x003BA -> // κ [GREEK SMALL LETTER KAPPA]
                new int[] {
                    0x003F0, // ϰ [GREEK KAPPA SYMBOL]
                    0x0039A, // Κ [GREEK CAPITAL LETTER KAPPA]
                };
            case 0x003BC -> // μ [GREEK SMALL LETTER MU]
                new int[] {
                    0x000B5, // µ [MICRO SIGN]
                    0x0039C, // Μ [GREEK CAPITAL LETTER MU]
                };
            case 0x003C0 -> // π [GREEK SMALL LETTER PI]
                new int[] {
                    0x003A0, // Π [GREEK CAPITAL LETTER PI]
                    0x003D6, // ϖ [GREEK PI SYMBOL]
                };
            case 0x003C1 -> // ρ [GREEK SMALL LETTER RHO]
                new int[] {
                    0x003A1, // Ρ [GREEK CAPITAL LETTER RHO]
                    0x003F1, // ϱ [GREEK RHO SYMBOL]
                };
            case 0x003C2 -> // ς [GREEK SMALL LETTER FINAL SIGMA]
                new int[] {
                    0x003A3, // Σ [GREEK CAPITAL LETTER SIGMA]
                    0x003C3, // σ [GREEK SMALL LETTER SIGMA]
                };
            case 0x003C3 -> // σ [GREEK SMALL LETTER SIGMA]
                new int[] {
                    0x003C2, // ς [GREEK SMALL LETTER FINAL SIGMA]
                    0x003A3, // Σ [GREEK CAPITAL LETTER SIGMA]
                };
            case 0x003C6 -> // φ [GREEK SMALL LETTER PHI]
                new int[] {
                    0x003D5, // ϕ [GREEK PHI SYMBOL]
                    0x003A6, // Φ [GREEK CAPITAL LETTER PHI]
                };
            case 0x003C9 -> // ω [GREEK SMALL LETTER OMEGA]
                new int[] {
                    0x02126, // Ω [OHM SIGN]
                    0x003A9, // Ω [GREEK CAPITAL LETTER OMEGA]
                };
            case 0x003D0 -> // ϐ [GREEK BETA SYMBOL]
                new int[] {
                    0x00392, // Β [GREEK CAPITAL LETTER BETA]
                    0x003B2, // β [GREEK SMALL LETTER BETA]
                };
            case 0x003D1 -> // ϑ [GREEK THETA SYMBOL]
                new int[] {
                    0x003F4, // ϴ [GREEK CAPITAL THETA SYMBOL]
                    0x00398, // Θ [GREEK CAPITAL LETTER THETA]
                    0x003B8, // θ [GREEK SMALL LETTER THETA]
                };
            case 0x003D5 -> // ϕ [GREEK PHI SYMBOL]
                new int[] {
                    0x003A6, // Φ [GREEK CAPITAL LETTER PHI]
                    0x003C6, // φ [GREEK SMALL LETTER PHI]
                };
            case 0x003D6 -> // ϖ [GREEK PI SYMBOL]
                new int[] {
                    0x003A0, // Π [GREEK CAPITAL LETTER PI]
                    0x003C0, // π [GREEK SMALL LETTER PI]
                };
            case 0x003F0 -> // ϰ [GREEK KAPPA SYMBOL]
                new int[] {
                    0x0039A, // Κ [GREEK CAPITAL LETTER KAPPA]
                    0x003BA, // κ [GREEK SMALL LETTER KAPPA]
                };
            case 0x003F1 -> // ϱ [GREEK RHO SYMBOL]
                new int[] {
                    0x003A1, // Ρ [GREEK CAPITAL LETTER RHO]
                    0x003C1, // ρ [GREEK SMALL LETTER RHO]
                };
            case 0x003F4 -> // ϴ [GREEK CAPITAL THETA SYMBOL]
                new int[] {
                    0x003D1, // ϑ [GREEK THETA SYMBOL]
                    0x00398, // Θ [GREEK CAPITAL LETTER THETA]
                    0x003B8, // θ [GREEK SMALL LETTER THETA]
                };
            case 0x003F5 -> // ϵ [GREEK LUNATE EPSILON SYMBOL]
                new int[] {
                    0x00395, // Ε [GREEK CAPITAL LETTER EPSILON]
                    0x003B5, // ε [GREEK SMALL LETTER EPSILON]
                };
            case 0x00412 -> // В [CYRILLIC CAPITAL LETTER VE]
                new int[] {
                    0x01C80, // ᲀ [CYRILLIC SMALL LETTER ROUNDED VE]
                    0x00432, // в [CYRILLIC SMALL LETTER VE]
                };
            case 0x00414 -> // Д [CYRILLIC CAPITAL LETTER DE]
                new int[] {
                    0x01C81, // ᲁ [CYRILLIC SMALL LETTER LONG-LEGGED DE]
                    0x00434, // д [CYRILLIC SMALL LETTER DE]
                };
            case 0x0041E -> // О [CYRILLIC CAPITAL LETTER O]
                new int[] {
                    0x01C82, // ᲂ [CYRILLIC SMALL LETTER NARROW O]
                    0x0043E, // о [CYRILLIC SMALL LETTER O]
                };
            case 0x00421 -> // С [CYRILLIC CAPITAL LETTER ES]
                new int[] {
                    0x00441, // с [CYRILLIC SMALL LETTER ES]
                    0x01C83, // ᲃ [CYRILLIC SMALL LETTER WIDE ES]
                };
            case 0x00422 -> // Т [CYRILLIC CAPITAL LETTER TE]
                new int[] {
                    0x00442, // т [CYRILLIC SMALL LETTER TE]
                    0x01C84, // ᲄ [CYRILLIC SMALL LETTER TALL TE]
                    0x01C85, // ᲅ [CYRILLIC SMALL LETTER THREE-LEGGED TE]
                };
            case 0x0042A -> // Ъ [CYRILLIC CAPITAL LETTER HARD SIGN]
                new int[] {
                    0x01C86, // ᲆ [CYRILLIC SMALL LETTER TALL HARD SIGN]
                    0x0044A, // ъ [CYRILLIC SMALL LETTER HARD SIGN]
                };
            case 0x00432 -> // в [CYRILLIC SMALL LETTER VE]
                new int[] {
                    0x01C80, // ᲀ [CYRILLIC SMALL LETTER ROUNDED VE]
                    0x00412, // В [CYRILLIC CAPITAL LETTER VE]
                };
            case 0x00434 -> // д [CYRILLIC SMALL LETTER DE]
                new int[] {
                    0x01C81, // ᲁ [CYRILLIC SMALL LETTER LONG-LEGGED DE]
                    0x00414, // Д [CYRILLIC CAPITAL LETTER DE]
                };
            case 0x0043E -> // о [CYRILLIC SMALL LETTER O]
                new int[] {
                    0x01C82, // ᲂ [CYRILLIC SMALL LETTER NARROW O]
                    0x0041E, // О [CYRILLIC CAPITAL LETTER O]
                };
            case 0x00441 -> // с [CYRILLIC SMALL LETTER ES]
                new int[] {
                    0x00421, // С [CYRILLIC CAPITAL LETTER ES]
                    0x01C83, // ᲃ [CYRILLIC SMALL LETTER WIDE ES]
                };
            case 0x00442 -> // т [CYRILLIC SMALL LETTER TE]
                new int[] {
                    0x00422, // Т [CYRILLIC CAPITAL LETTER TE]
                    0x01C84, // ᲄ [CYRILLIC SMALL LETTER TALL TE]
                    0x01C85, // ᲅ [CYRILLIC SMALL LETTER THREE-LEGGED TE]
                };
            case 0x0044A -> // ъ [CYRILLIC SMALL LETTER HARD SIGN]
                new int[] {
                    0x01C86, // ᲆ [CYRILLIC SMALL LETTER TALL HARD SIGN]
                    0x0042A, // Ъ [CYRILLIC CAPITAL LETTER HARD SIGN]
                };
            case 0x00462 -> // Ѣ [CYRILLIC CAPITAL LETTER YAT]
                new int[] {
                    0x00463, // ѣ [CYRILLIC SMALL LETTER YAT]
                    0x01C87, // ᲇ [CYRILLIC SMALL LETTER TALL YAT]
                };
            case 0x00463 -> // ѣ [CYRILLIC SMALL LETTER YAT]
                new int[] {
                    0x00462, // Ѣ [CYRILLIC CAPITAL LETTER YAT]
                    0x01C87, // ᲇ [CYRILLIC SMALL LETTER TALL YAT]
                };
            case 0x0A64A -> // Ꙋ [CYRILLIC CAPITAL LETTER MONOGRAPH UK]
                new int[] {
                    0x01C88, // ᲈ [CYRILLIC SMALL LETTER UNBLENDED UK]
                    0x0A64B, // ꙋ [CYRILLIC SMALL LETTER MONOGRAPH UK]
                };
            case 0x0A64B -> // ꙋ [CYRILLIC SMALL LETTER MONOGRAPH UK]
                new int[] {
                    0x01C88, // ᲈ [CYRILLIC SMALL LETTER UNBLENDED UK]
                    0x0A64A, // Ꙋ [CYRILLIC CAPITAL LETTER MONOGRAPH UK]
                };
            case 0x0A7CB -> // Ɤ [LATIN CAPITAL LETTER RAMS HORN]
                new int[] { 0x00264, // ɤ [LATIN SMALL LETTER RAMS HORN]
                };
            case 0x0A7CC -> // Ꟍ [LATIN CAPITAL LETTER S WITH DIAGONAL STROKE]
                new int[] { 0x0A7CD, // ꟍ [LATIN SMALL LETTER S WITH DIAGONAL STROKE]
                };
            case 0x0A7CD -> // ꟍ [LATIN SMALL LETTER S WITH DIAGONAL STROKE]
                new int[] { 0x0A7CC, // Ꟍ [LATIN CAPITAL LETTER S WITH DIAGONAL STROKE]
                };
            case 0x0A7DA -> // Ꟛ [LATIN CAPITAL LETTER LAMBDA]
                new int[] { 0x0A7DB, // ꟛ [LATIN SMALL LETTER LAMBDA]
                };
            case 0x0A7DB -> // ꟛ [LATIN SMALL LETTER LAMBDA]
                new int[] { 0x0A7DA, // Ꟛ [LATIN CAPITAL LETTER LAMBDA]
                };
            case 0x0A7DC -> // Ƛ [LATIN CAPITAL LETTER LAMBDA WITH STROKE]
                new int[] { 0x0019B, // ƛ [LATIN SMALL LETTER LAMBDA WITH STROKE]
                };
            case 0x0FB05 -> // ﬅ [LATIN SMALL LIGATURE LONG S T]
                new int[] { 0x0FB06, // ﬆ [LATIN SMALL LIGATURE ST]
                };
            case 0x0FB06 -> // ﬆ [LATIN SMALL LIGATURE ST]
                new int[] { 0x0FB05, // ﬅ [LATIN SMALL LIGATURE LONG S T]
                };
            case 0x01C80 -> // ᲀ [CYRILLIC SMALL LETTER ROUNDED VE]
                new int[] {
                    0x00412, // В [CYRILLIC CAPITAL LETTER VE]
                    0x00432, // в [CYRILLIC SMALL LETTER VE]
                };
            case 0x01C81 -> // ᲁ [CYRILLIC SMALL LETTER LONG-LEGGED DE]
                new int[] {
                    0x00414, // Д [CYRILLIC CAPITAL LETTER DE]
                    0x00434, // д [CYRILLIC SMALL LETTER DE]
                };
            case 0x01C82 -> // ᲂ [CYRILLIC SMALL LETTER NARROW O]
                new int[] {
                    0x0041E, // О [CYRILLIC CAPITAL LETTER O]
                    0x0043E, // о [CYRILLIC SMALL LETTER O]
                };
            case 0x01C83 -> // ᲃ [CYRILLIC SMALL LETTER WIDE ES]
                new int[] {
                    0x00421, // С [CYRILLIC CAPITAL LETTER ES]
                    0x00441, // с [CYRILLIC SMALL LETTER ES]
                };
            case 0x01C84 -> // ᲄ [CYRILLIC SMALL LETTER TALL TE]
                new int[] {
                    0x00422, // Т [CYRILLIC CAPITAL LETTER TE]
                    0x00442, // т [CYRILLIC SMALL LETTER TE]
                    0x01C85, // ᲅ [CYRILLIC SMALL LETTER THREE-LEGGED TE]
                };
            case 0x01C85 -> // ᲅ [CYRILLIC SMALL LETTER THREE-LEGGED TE]
                new int[] {
                    0x00422, // Т [CYRILLIC CAPITAL LETTER TE]
                    0x00442, // т [CYRILLIC SMALL LETTER TE]
                    0x01C84, // ᲄ [CYRILLIC SMALL LETTER TALL TE]
                };
            case 0x01C86 -> // ᲆ [CYRILLIC SMALL LETTER TALL HARD SIGN]
                new int[] {
                    0x0042A, // Ъ [CYRILLIC CAPITAL LETTER HARD SIGN]
                    0x0044A, // ъ [CYRILLIC SMALL LETTER HARD SIGN]
                };
            case 0x01C87 -> // ᲇ [CYRILLIC SMALL LETTER TALL YAT]
                new int[] {
                    0x00462, // Ѣ [CYRILLIC CAPITAL LETTER YAT]
                    0x00463, // ѣ [CYRILLIC SMALL LETTER YAT]
                };
            case 0x01C88 -> // ᲈ [CYRILLIC SMALL LETTER UNBLENDED UK]
                new int[] {
                    0x0A64A, // Ꙋ [CYRILLIC CAPITAL LETTER MONOGRAPH UK]
                    0x0A64B, // ꙋ [CYRILLIC SMALL LETTER MONOGRAPH UK]
                };
            case 0x01C89 -> // Ᲊ [CYRILLIC CAPITAL LETTER TJE]
                new int[] { 0x01C8A, // ᲊ [CYRILLIC SMALL LETTER TJE]
                };
            case 0x01C8A -> // ᲊ [CYRILLIC SMALL LETTER TJE]
                new int[] { 0x01C89, // Ᲊ [CYRILLIC CAPITAL LETTER TJE]
                };
            case 0x10D51 -> // 𐵑 [GARAY CAPITAL LETTER CA]
                new int[] { 0x10D71, // 𐵱 [GARAY SMALL LETTER CA]
                };
            case 0x10D50 -> // 𐵐 [GARAY CAPITAL LETTER A]
                new int[] { 0x10D70, // 𐵰 [GARAY SMALL LETTER A]
                };
            case 0x10D53 -> // 𐵓 [GARAY CAPITAL LETTER KA]
                new int[] { 0x10D73, // 𐵳 [GARAY SMALL LETTER KA]
                };
            case 0x10D52 -> // 𐵒 [GARAY CAPITAL LETTER MA]
                new int[] { 0x10D72, // 𐵲 [GARAY SMALL LETTER MA]
                };
            case 0x10D55 -> // 𐵕 [GARAY CAPITAL LETTER JA]
                new int[] { 0x10D75, // 𐵵 [GARAY SMALL LETTER JA]
                };
            case 0x10D54 -> // 𐵔 [GARAY CAPITAL LETTER BA]
                new int[] { 0x10D74, // 𐵴 [GARAY SMALL LETTER BA]
                };
            case 0x10D57 -> // 𐵗 [GARAY CAPITAL LETTER WA]
                new int[] { 0x10D77, // 𐵷 [GARAY SMALL LETTER WA]
                };
            case 0x10D56 -> // 𐵖 [GARAY CAPITAL LETTER SA]
                new int[] { 0x10D76, // 𐵶 [GARAY SMALL LETTER SA]
                };
            case 0x10D59 -> // 𐵙 [GARAY CAPITAL LETTER GA]
                new int[] { 0x10D79, // 𐵹 [GARAY SMALL LETTER GA]
                };
            case 0x10D58 -> // 𐵘 [GARAY CAPITAL LETTER LA]
                new int[] { 0x10D78, // 𐵸 [GARAY SMALL LETTER LA]
                };
            case 0x10D5B -> // 𐵛 [GARAY CAPITAL LETTER XA]
                new int[] { 0x10D7B, // 𐵻 [GARAY SMALL LETTER XA]
                };
            case 0x10D5A -> // 𐵚 [GARAY CAPITAL LETTER DA]
                new int[] { 0x10D7A, // 𐵺 [GARAY SMALL LETTER DA]
                };
            case 0x10D5D -> // 𐵝 [GARAY CAPITAL LETTER TA]
                new int[] { 0x10D7D, // 𐵽 [GARAY SMALL LETTER TA]
                };
            case 0x10D5C -> // 𐵜 [GARAY CAPITAL LETTER YA]
                new int[] { 0x10D7C, // 𐵼 [GARAY SMALL LETTER YA]
                };
            case 0x10D5F -> // 𐵟 [GARAY CAPITAL LETTER NYA]
                new int[] { 0x10D7F, // 𐵿 [GARAY SMALL LETTER NYA]
                };
            case 0x10D5E -> // 𐵞 [GARAY CAPITAL LETTER RA]
                new int[] { 0x10D7E, // 𐵾 [GARAY SMALL LETTER RA]
                };
            case 0x10D61 -> // 𐵡 [GARAY CAPITAL LETTER NA]
                new int[] { 0x10D81, // 𐶁 [GARAY SMALL LETTER NA]
                };
            case 0x10D60 -> // 𐵠 [GARAY CAPITAL LETTER FA]
                new int[] { 0x10D80, // 𐶀 [GARAY SMALL LETTER FA]
                };
            case 0x10D63 -> // 𐵣 [GARAY CAPITAL LETTER HA]
                new int[] { 0x10D83, // 𐶃 [GARAY SMALL LETTER HA]
                };
            case 0x10D62 -> // 𐵢 [GARAY CAPITAL LETTER PA]
                new int[] { 0x10D82, // 𐶂 [GARAY SMALL LETTER PA]
                };
            case 0x10D65 -> // 𐵥 [GARAY CAPITAL LETTER OLD NA]
                new int[] { 0x10D85, // 𐶅 [GARAY SMALL LETTER OLD NA]
                };
            case 0x10D64 -> // 𐵤 [GARAY CAPITAL LETTER OLD KA]
                new int[] { 0x10D84, // 𐶄 [GARAY SMALL LETTER OLD KA]
                };
            case 0x10D71 -> // 𐵱 [GARAY SMALL LETTER CA]
                new int[] { 0x10D51, // 𐵑 [GARAY CAPITAL LETTER CA]
                };
            case 0x10D70 -> // 𐵰 [GARAY SMALL LETTER A]
                new int[] { 0x10D50, // 𐵐 [GARAY CAPITAL LETTER A]
                };
            case 0x10D73 -> // 𐵳 [GARAY SMALL LETTER KA]
                new int[] { 0x10D53, // 𐵓 [GARAY CAPITAL LETTER KA]
                };
            case 0x10D72 -> // 𐵲 [GARAY SMALL LETTER MA]
                new int[] { 0x10D52, // 𐵒 [GARAY CAPITAL LETTER MA]
                };
            case 0x10D75 -> // 𐵵 [GARAY SMALL LETTER JA]
                new int[] { 0x10D55, // 𐵕 [GARAY CAPITAL LETTER JA]
                };
            case 0x10D74 -> // 𐵴 [GARAY SMALL LETTER BA]
                new int[] { 0x10D54, // 𐵔 [GARAY CAPITAL LETTER BA]
                };
            case 0x10D77 -> // 𐵷 [GARAY SMALL LETTER WA]
                new int[] { 0x10D57, // 𐵗 [GARAY CAPITAL LETTER WA]
                };
            case 0x10D76 -> // 𐵶 [GARAY SMALL LETTER SA]
                new int[] { 0x10D56, // 𐵖 [GARAY CAPITAL LETTER SA]
                };
            case 0x10D79 -> // 𐵹 [GARAY SMALL LETTER GA]
                new int[] { 0x10D59, // 𐵙 [GARAY CAPITAL LETTER GA]
                };
            case 0x10D78 -> // 𐵸 [GARAY SMALL LETTER LA]
                new int[] { 0x10D58, // 𐵘 [GARAY CAPITAL LETTER LA]
                };
            case 0x10D7B -> // 𐵻 [GARAY SMALL LETTER XA]
                new int[] { 0x10D5B, // 𐵛 [GARAY CAPITAL LETTER XA]
                };
            case 0x10D7A -> // 𐵺 [GARAY SMALL LETTER DA]
                new int[] { 0x10D5A, // 𐵚 [GARAY CAPITAL LETTER DA]
                };
            case 0x10D7D -> // 𐵽 [GARAY SMALL LETTER TA]
                new int[] { 0x10D5D, // 𐵝 [GARAY CAPITAL LETTER TA]
                };
            case 0x10D7C -> // 𐵼 [GARAY SMALL LETTER YA]
                new int[] { 0x10D5C, // 𐵜 [GARAY CAPITAL LETTER YA]
                };
            case 0x10D7F -> // 𐵿 [GARAY SMALL LETTER NYA]
                new int[] { 0x10D5F, // 𐵟 [GARAY CAPITAL LETTER NYA]
                };
            case 0x10D7E -> // 𐵾 [GARAY SMALL LETTER RA]
                new int[] { 0x10D5E, // 𐵞 [GARAY CAPITAL LETTER RA]
                };
            case 0x10D81 -> // 𐶁 [GARAY SMALL LETTER NA]
                new int[] { 0x10D61, // 𐵡 [GARAY CAPITAL LETTER NA]
                };
            case 0x10D80 -> // 𐶀 [GARAY SMALL LETTER FA]
                new int[] { 0x10D60, // 𐵠 [GARAY CAPITAL LETTER FA]
                };
            case 0x10D83 -> // 𐶃 [GARAY SMALL LETTER HA]
                new int[] { 0x10D63, // 𐵣 [GARAY CAPITAL LETTER HA]
                };
            case 0x10D82 -> // 𐶂 [GARAY SMALL LETTER PA]
                new int[] { 0x10D62, // 𐵢 [GARAY CAPITAL LETTER PA]
                };
            case 0x10D85 -> // 𐶅 [GARAY SMALL LETTER OLD NA]
                new int[] { 0x10D65, // 𐵥 [GARAY CAPITAL LETTER OLD NA]
                };
            case 0x10D84 -> // 𐶄 [GARAY SMALL LETTER OLD KA]
                new int[] { 0x10D64, // 𐵤 [GARAY CAPITAL LETTER OLD KA]
                };
            case 0x01E60 -> // Ṡ [LATIN CAPITAL LETTER S WITH DOT ABOVE]
                new int[] {
                    0x01E61, // ṡ [LATIN SMALL LETTER S WITH DOT ABOVE]
                    0x01E9B, // ẛ [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
                };
            case 0x01E61 -> // ṡ [LATIN SMALL LETTER S WITH DOT ABOVE]
                new int[] {
                    0x01E60, // Ṡ [LATIN CAPITAL LETTER S WITH DOT ABOVE]
                    0x01E9B, // ẛ [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
                };
            case 0x01E9B -> // ẛ [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
                new int[] {
                    0x01E60, // Ṡ [LATIN CAPITAL LETTER S WITH DOT ABOVE]
                    0x01E61, // ṡ [LATIN SMALL LETTER S WITH DOT ABOVE]
                };
            case 0x01FBE -> // ι [GREEK PROSGEGRAMMENI]
                new int[] {
                    0x00345, // ͅ [COMBINING GREEK YPOGEGRAMMENI]
                    0x00399, // Ι [GREEK CAPITAL LETTER IOTA]
                    0x003B9, // ι [GREEK SMALL LETTER IOTA]
                };
            case 0x01FD3 -> // ΐ [GREEK SMALL LETTER IOTA WITH DIALYTIKA AND OXIA]
                new int[] { 0x00390, // ΐ [GREEK SMALL LETTER IOTA WITH DIALYTIKA AND TONOS]
                };
            case 0x01FE3 -> // ΰ [GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND OXIA]
                new int[] { 0x003B0, // ΰ [GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND TONOS]
                };
            default -> null;
        };

        return alts;
    }

    static String normalizeCase(String str) {
        int[] lowestCodePoints = str.codePoints().map(CaseFolding::lowestCodePoint).toArray();
        return new String(lowestCodePoints, 0, lowestCodePoints.length);
    }
}
