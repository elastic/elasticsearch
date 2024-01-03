/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dissect;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Splits (dissects) a string into its parts based on a pattern.
 * <p>A dissect pattern is composed of a set of keys and delimiters.
 * For example the dissect pattern: <pre>%{a} %{b},%{c}</pre> has 3 keys (a,b,c) and two delimiters (space and comma). This pattern will
 * match a string of the form: <pre>foo bar,baz</pre> and will result a key/value pairing of <pre>a=foo, b=bar, and c=baz.</pre>
 * <p>Matches are all or nothing. For example, the same pattern will NOT match <pre>foo bar baz</pre> since all of the delimiters did not
 * match. (the comma did not match)
 * <p>Dissect patterns can optionally have modifiers. These modifiers instruct the parser to change its behavior. For example the
 * dissect pattern of <pre>%{a},%{b}:%{c}</pre> would not match <pre>foo,bar,baz</pre> since there the colon never matches.
 * <p>Modifiers appear to the left or the right of the key name. The supported modifiers are:
 * <ul>
 * <li>{@code ->} Instructs the parser to ignore repeating delimiters to the right of the key. Example: <pre>
 * pattern: {@code %{a->} %{b} %{c}}
 * string: {@code foo         bar baz}
 * result: {@code a=foo, b=bar, c=baz}
 * </pre></li>
 * <li>{@code +} Instructs the parser to appends this key's value to value of prior key with the same name.
 * Example: <pre>
 * pattern: {@code %{a} %{+a} %{+a}}
 * string: {@code foo bar baz}
 * result: {@code a=foobarbaz}
 * </pre></li>
 * <li>{@code /} Instructs the parser to appends this key's value to value of a key based based on the order specified after the
 * {@code /}. Requires the {@code +} modifier to also be present in the key. Example: <pre>
 * pattern: {@code %{a} %{+a/2} %{+a/1}}
 * string: {@code foo bar baz}
 * result: {@code a=foobazbar}
 * </pre>
 * </li>
 * <li>{@code *} Instructs the parser to ignore the name of this key, instead use the value of key as the key name.
 * Requires another key with the same name and the {@code &} modifier to be the value. Example: <pre>
 * pattern: {@code %{*a} %{b} %{&a}}
 * string: {@code foo bar baz}
 * result: {@code foo=baz, b=bar}
 * </pre></li>
 * <li>{@code &} Instructs the parser to ignore this key and place the matched value to a key of the same name with the {@code *} modifier.
 * Requires another key with the same name and the {@code *} modifier.
 * Example: <pre>
 * pattern: {@code %{*a} %{b} %{&a}}
 * string: {@code foo bar baz}
 * result: {@code foo=baz, b=bar}
 * </pre></li>
 * <li>{@code ?} Instructs the parser to ignore this key. The key name exists only for the purpose of human readability. Example
 * <pre>
 *  pattern: {@code %{a} %{?skipme} %{c}}
 *  string: {@code foo bar baz}
 *  result: {@code a=foo, c=baz}
 * </pre>
 * </ul>
 * <p>Empty key names patterns are also supported. They behave just like the {@code ?} modifier, except the name is not required.
 * The result will simply be ignored. Example
 * <pre>
 * pattern: {@code %{a} %{} %{c}}
 * string: {@code foo bar baz}
 * result: {@code a=foo, c=baz}
 * </pre>

 * <p>
 * Inspired by the Logstash Dissect Filter by Guy Boertje
 */
public final class DissectParser {
    private static final Pattern LEADING_DELIMITER_PATTERN = Pattern.compile("^(.*?)%\\{");
    private static final Pattern KEY_DELIMITER_FIELD_PATTERN = Pattern.compile("%\\{([^}]*?)}(.+?(?=%\\{)|.*$)", Pattern.DOTALL);
    private static final EnumSet<DissectKey.Modifier> ASSOCIATE_MODIFIERS = EnumSet.of(
        DissectKey.Modifier.FIELD_NAME,
        DissectKey.Modifier.FIELD_VALUE
    );
    private static final EnumSet<DissectKey.Modifier> APPEND_MODIFIERS = EnumSet.of(
        DissectKey.Modifier.APPEND,
        DissectKey.Modifier.APPEND_WITH_ORDER
    );
    private static final Function<DissectPair, String> KEY_NAME = val -> val.key().getName();
    private final List<DissectPair> matchPairs;
    private final String pattern;
    private String leadingDelimiter = "";
    private final int maxMatches;
    private final int maxResults;
    private final int appendCount;
    private final int referenceCount;
    private final String appendSeparator;

    public DissectParser(String pattern, String appendSeparator) {
        this.pattern = pattern;
        this.appendSeparator = appendSeparator == null ? "" : appendSeparator;
        Matcher matcher = LEADING_DELIMITER_PATTERN.matcher(pattern);
        while (matcher.find()) {
            leadingDelimiter = matcher.group(1);
        }
        List<DissectPair> dissectPairs = new ArrayList<>();
        matcher = KEY_DELIMITER_FIELD_PATTERN.matcher(pattern.substring(leadingDelimiter.length()));
        while (matcher.find()) {
            DissectKey key = new DissectKey(matcher.group(1));
            String delimiter = matcher.group(2);
            dissectPairs.add(new DissectPair(key, delimiter));
        }
        this.maxMatches = dissectPairs.size();
        this.maxResults = Long.valueOf(
            dissectPairs.stream().filter(dissectPair -> dissectPair.key().skip() == false).map(KEY_NAME).distinct().count()
        ).intValue();
        if (this.maxMatches == 0 || maxResults == 0) {
            throw new DissectException.PatternParse(pattern, "Unable to find any keys or delimiters.");
        }
        // append validation - look through all of the keys to see if there are any keys that need to participate in an append operation
        // but don't have the '+' defined
        Set<String> appendKeyNames = dissectPairs.stream()
            .filter(dissectPair -> APPEND_MODIFIERS.contains(dissectPair.key().getModifier()))
            .map(KEY_NAME)
            .collect(Collectors.toSet());
        if (appendKeyNames.size() > 0) {
            List<DissectPair> modifiedMatchPairs = new ArrayList<>(dissectPairs.size());
            for (DissectPair p : dissectPairs) {
                if (p.key().getModifier().equals(DissectKey.Modifier.NONE) && appendKeyNames.contains(p.key().getName())) {
                    modifiedMatchPairs.add(new DissectPair(new DissectKey(p.key(), DissectKey.Modifier.APPEND), p.delimiter()));
                } else {
                    modifiedMatchPairs.add(p);
                }
            }
            dissectPairs = modifiedMatchPairs;
        }
        appendCount = appendKeyNames.size();

        // reference validation - ensure that '*' and '&' come in pairs
        Map<String, List<DissectPair>> referenceGroupings = dissectPairs.stream()
            .filter(dissectPair -> ASSOCIATE_MODIFIERS.contains(dissectPair.key().getModifier()))
            .collect(Collectors.groupingBy(KEY_NAME));
        for (Map.Entry<String, List<DissectPair>> entry : referenceGroupings.entrySet()) {
            if (entry.getValue().size() != 2) {
                throw new DissectException.PatternParse(
                    pattern,
                    "Found invalid key/reference associations: '"
                        + entry.getValue().stream().map(KEY_NAME).collect(Collectors.joining(","))
                        + "' Please ensure each '*<key>' is matched with a matching '&<key>"
                );
            }
        }

        referenceCount = referenceGroupings.size() * 2;
        this.matchPairs = List.copyOf(dissectPairs);
    }

    /**
     * Entry point to dissect a string into its parts.
     *
     * @param inputString The string to dissect
     * @return the key/value Map of the results
     * @throws DissectException if unable to dissect a pair into its parts.
     */
    public Map<String, String> parse(String inputString) {
        /**
         *
         * This implements a naive string matching algorithm. The string is walked left to right, comparing each byte against
         * another string's bytes looking for matches. If the bytes match, then a second cursor looks ahead to see if all the bytes
         * of the other string matches. If they all match, record it and advances the primary cursor to the match point. If it can not match
         * all of the bytes then progress the main cursor. Repeat till the end of the input string. Since the string being searching for
         * (the delimiter) is generally small and rare the naive approach is efficient.
         *
         * In this case the string that is walked is the input string, and the string being searched for is the current delimiter.
         * For example for a dissect pattern of {@code %{a},%{b}:%{c}} the delimiters (comma then colon) are searched for in the
         * input string. At class construction the list of keys+delimiters are found (dissectPairs), which allows the use of that ordered
         * list to know which delimiter to use for the search. The delimiters is progressed once the current delimiter is matched.
         *
         * There are two special cases that requires additional parsing beyond the standard naive algorithm. Consecutive delimiters should
         * results in a empty matches unless the {@code ->} is provided. For example given the dissect pattern of
         * {@code %{a},%{b},%{c},%{d}} and input string of {@code foo,,,} the match should be successful with empty values for b,c and d.
         * However, if the key modifier {@code ->}, is present it will simply skip over any delimiters just to the right of the key
         * without assigning any values. For example {@code %{a->},{%b}} will match the input string of {@code foo,,,,,,bar} with a=foo and
         * b=bar.
         *
         */
        DissectMatch dissectMatch = new DissectMatch(appendSeparator, maxMatches, maxResults, appendCount, referenceCount);
        Iterator<DissectPair> it = matchPairs.iterator();
        // ensure leading delimiter matches
        if (inputString != null
            && inputString.length() > leadingDelimiter.length()
            && leadingDelimiter.equals(inputString.substring(0, leadingDelimiter.length()))) {
            byte[] input = inputString.getBytes(StandardCharsets.UTF_8);
            // grab the first key/delimiter pair
            DissectPair dissectPair = it.next();
            DissectKey key = dissectPair.key();
            byte[] delimiter = dissectPair.delimiter().getBytes(StandardCharsets.UTF_8);
            // start dissection after the first delimiter
            int i = leadingDelimiter.length();
            int valueStart = i;
            int lookAheadMatches;
            // start walking the input string byte by byte, look ahead for matches where needed
            // if a match is found jump forward to the end of the match
            while (i < input.length) {
                lookAheadMatches = 0;
                // potential match between delimiter and input string
                if (delimiter.length > 0 && input[i] == delimiter[0]) {
                    // look ahead to see if the entire delimiter matches the input string
                    for (int j = 0; j < delimiter.length; j++) {
                        if (i + j < input.length && input[i + j] == delimiter[j]) {
                            lookAheadMatches++;
                        }
                    }
                    // found a full delimiter match
                    if (lookAheadMatches == delimiter.length) {
                        // record the key/value tuple
                        byte[] value = Arrays.copyOfRange(input, valueStart, i);
                        dissectMatch.add(key, new String(value, StandardCharsets.UTF_8));
                        // jump to the end of the match
                        i += lookAheadMatches;
                        // look for consecutive delimiters (e.g. a,,,,d,e)
                        while (i < input.length) {
                            lookAheadMatches = 0;
                            for (int j = 0; j < delimiter.length; j++) {
                                if (i + j < input.length && input[i + j] == delimiter[j]) {
                                    lookAheadMatches++;
                                }
                            }
                            // found consecutive delimiters
                            if (lookAheadMatches == delimiter.length) {
                                // jump to the end of the match
                                i += lookAheadMatches;
                                if (key.skipRightPadding() == false) {
                                    // progress the keys/delimiter if possible
                                    if (it.hasNext() == false) {
                                        break; // the while loop
                                    }
                                    dissectPair = it.next();
                                    key = dissectPair.key();
                                    // add the key with an empty value for the empty delimiter
                                    dissectMatch.add(key, "");
                                }
                            } else {
                                break; // the while loop
                            }
                        }
                        // progress the keys/delimiter if possible
                        if (it.hasNext() == false) {
                            break; // the for loop
                        }
                        dissectPair = it.next();
                        key = dissectPair.key();
                        delimiter = dissectPair.delimiter().getBytes(StandardCharsets.UTF_8);
                        // i is always one byte after the last found delimiter, aka the start of the next value
                        valueStart = i;
                    } else {
                        i++;
                    }
                } else {
                    i++;
                }
            }
            // the last key, grab the rest of the input (unless consecutive delimiters already grabbed the last key)
            // and there is no trailing delimiter
            if (dissectMatch.fullyMatched() == false && delimiter.length == 0) {
                byte[] value = Arrays.copyOfRange(input, valueStart, input.length);
                String valueString = new String(value, StandardCharsets.UTF_8);
                dissectMatch.add(key, valueString);
            }
        }
        Map<String, String> results = dissectMatch.getResults();

        return dissectMatch.isValid(results) ? results : null;
    }

    /**
     * Entry point to dissect a string into its parts.
     *
     * @param inputString The string to dissect
     * @return the key/value Map of the results
     * @throws DissectException if unable to dissect a pair into its parts.
     */
    public Map<String, String> forceParse(String inputString) {
        Map<String, String> results = parse(inputString);
        if (results == null) {
            throw new DissectException.FindMatch(pattern, inputString);
        }
        return results;
    }

    /**
     * Returns the output keys produced by the instance (excluding named skip keys),
     * e.g. for the pattern <code>"%{a} %{b} %{?c}"</code> the result is <code>[a, b]</code>.
     * <p>
     * The result is an ordered set, where the entries are in the same order as they appear in the pattern.
     * <p>
     * The reference keys are returned with the name they have in the pattern, e.g. for <code>"%{*x} %{&amp;x}"</code>
     * the result is <code>[x]</code>.
     *
     * @return the output keys produced by the instance.
     */
    public Set<String> outputKeys() {
        Set<String> result = new LinkedHashSet<>(matchPairs.size());
        for (DissectPair matchPair : matchPairs) {
            if (matchPair.key.getModifier() != DissectKey.Modifier.NAMED_SKIP) {
                result.add(matchPair.key.getName());
            }
        }
        return result;
    }

    /**
     * Returns the reference keys present in the pattern,
     * e.g. for the pattern <code>"%{a} %{b} %{*c} %{&amp;c} %{*d} %{&amp;d}"</code> it returns <code>[c, d]</code>.
     * <p>
     * The result is an ordered set, where the entries are in the same order as they appear in the pattern.
     *
     * @return the reference keys included in the pattern.
     */
    public Set<String> referenceKeys() {
        Set<String> result = new LinkedHashSet<>(matchPairs.size());
        for (DissectPair matchPair : matchPairs) {
            if (matchPair.key.getModifier() == DissectKey.Modifier.FIELD_NAME) {
                result.add(matchPair.key.getName());
            }
        }
        return result;
    }

    /**
     * A tuple class to hold the dissect key and delimiter
     */
    private record DissectPair(DissectKey key, String delimiter) {}

}
