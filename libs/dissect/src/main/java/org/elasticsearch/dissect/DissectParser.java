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

package org.elasticsearch.dissect;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>Splits (dissects) a string into its parts based on a pattern.</p><p>A dissect pattern is composed of a set of keys and delimiters.
 * For example the dissect pattern: <pre>%{a} %{b},%{c}</pre> has 3 keys (a,b,c) and two delimiters (space and comma). This pattern will
 * match a string of the form: <pre>foo bar,baz</pre> and will result a key/value pairing of <pre>a=foo, b=bar, and c=baz.</pre>
 * <p>Matches are all or nothing. For example, the same pattern will NOT match <pre>foo bar baz</pre> since all of the delimiters did not
 * match. (the comma did not match)
 * <p>Dissect patterns can optionally have modifiers. These modifiers instruct the parser to change it's behavior. For example the
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
    private static final Pattern LEADING_DELIMITER_PATTERN = Pattern.compile("^(.*?)%");
    private static final Pattern KEY_DELIMITER_FIELD_PATTERN = Pattern.compile("%\\{([^}]*?)}([^%]*)", Pattern.DOTALL);
    private static final EnumSet<DissectKey.Modifier> ASSOCIATE_MODIFIERS = EnumSet.of(
        DissectKey.Modifier.FIELD_NAME,
        DissectKey.Modifier.FIELD_VALUE);
    private static final EnumSet<DissectKey.Modifier> APPEND_MODIFIERS = EnumSet.of(
        DissectKey.Modifier.APPEND,
        DissectKey.Modifier.APPEND_WITH_ORDER);
    private static final Function<DissectPair, String> KEY_NAME = val -> val.getKey().getName();
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
        List<DissectPair> matchPairs = new ArrayList<>();
        matcher = KEY_DELIMITER_FIELD_PATTERN.matcher(pattern.substring(leadingDelimiter.length()));
        while (matcher.find()) {
            DissectKey key = new DissectKey(matcher.group(1));
            String delimiter = matcher.group(2);
            matchPairs.add(new DissectPair(key, delimiter));
        }
        this.maxMatches = matchPairs.size();
        this.maxResults = Long.valueOf(matchPairs.stream()
            .filter(dissectPair -> !dissectPair.getKey().skip()).map(KEY_NAME).distinct().count()).intValue();
        if (this.maxMatches == 0 || maxResults == 0) {
            throw new DissectException.PatternParse(pattern, "Unable to find any keys or delimiters.");
        }
        //append validation - look through all of the keys to see if there are any keys that need to participate in an append operation
        // but don't have the '+' defined
        Set<String> appendKeyNames = matchPairs.stream()
            .filter(dissectPair -> APPEND_MODIFIERS.contains(dissectPair.getKey().getModifier()))
            .map(KEY_NAME).distinct().collect(Collectors.toSet());
        if (appendKeyNames.size() > 0) {
            List<DissectPair> modifiedMatchPairs = new ArrayList<>(matchPairs.size());
            for (DissectPair p : matchPairs) {
                if (p.getKey().getModifier().equals(DissectKey.Modifier.NONE) && appendKeyNames.contains(p.getKey().getName())) {
                    modifiedMatchPairs.add(new DissectPair(new DissectKey(p.getKey(), DissectKey.Modifier.APPEND), p.getDelimiter()));
                } else {
                    modifiedMatchPairs.add(p);
                }
            }
            matchPairs = modifiedMatchPairs;
        }
        appendCount = appendKeyNames.size();

        //reference validation - ensure that '*' and '&' come in pairs
        Map<String, List<DissectPair>> referenceGroupings = matchPairs.stream()
            .filter(dissectPair -> ASSOCIATE_MODIFIERS.contains(dissectPair.getKey().getModifier()))
            .collect(Collectors.groupingBy(KEY_NAME));
        for (Map.Entry<String, List<DissectPair>> entry : referenceGroupings.entrySet()) {
            if (entry.getValue().size() != 2) {
                throw new DissectException.PatternParse(pattern, "Found invalid key/reference associations: '"
                    + entry.getValue().stream().map(KEY_NAME).collect(Collectors.joining(",")) +
                    "' Please ensure each '*<key>' is matched with a matching '&<key>");
            }
        }

        referenceCount = referenceGroupings.size() * 2;
        this.matchPairs = Collections.unmodifiableList(matchPairs);
    }


    /**
     * <p>Entry point to dissect a string into it's parts.</p>
     *
     * @param inputString The string to dissect
     * @return the key/value Map of the results
     * @throws DissectException if unable to dissect a pair into it's parts.
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
        //ensure leading delimiter matches
        if (inputString != null && inputString.length() > leadingDelimiter.length()
            && leadingDelimiter.equals(inputString.substring(0, leadingDelimiter.length()))) {
            byte[] input = inputString.getBytes(StandardCharsets.UTF_8);
            //grab the first key/delimiter pair
            DissectPair dissectPair = it.next();
            DissectKey key = dissectPair.getKey();
            byte[] delimiter = dissectPair.getDelimiter().getBytes(StandardCharsets.UTF_8);
            //start dissection after the first delimiter
            int i = leadingDelimiter.length();
            int valueStart = i;
            int lookAheadMatches;
            //start walking the input string byte by byte, look ahead for matches where needed
            //if a match is found jump forward to the end of the match
            for (; i < input.length; i++) {
                lookAheadMatches = 0;
                //potential match between delimiter and input string
                if (delimiter.length > 0 && input[i] == delimiter[0]) {
                    //look ahead to see if the entire delimiter matches the input string
                    for (int j = 0; j < delimiter.length; j++) {
                        if (i + j < input.length && input[i + j] == delimiter[j]) {
                            lookAheadMatches++;
                        }
                    }
                    //found a full delimiter match
                    if (lookAheadMatches == delimiter.length) {
                        //record the key/value tuple
                        byte[] value = Arrays.copyOfRange(input, valueStart, i);
                        dissectMatch.add(key, new String(value, StandardCharsets.UTF_8));
                        //jump to the end of the match
                        i += lookAheadMatches;
                        //look for consecutive delimiters (e.g. a,,,,d,e)
                        while (i < input.length) {
                            lookAheadMatches = 0;
                            for (int j = 0; j < delimiter.length; j++) {
                                if (i + j < input.length && input[i + j] == delimiter[j]) {
                                    lookAheadMatches++;
                                }
                            }
                            //found consecutive delimiters
                            if (lookAheadMatches == delimiter.length) {
                                //jump to the end of the match
                                i += lookAheadMatches;
                                if (!key.skipRightPadding()) {
                                    //progress the keys/delimiter if possible
                                    if (!it.hasNext()) {
                                        break; //the while loop
                                    }
                                    dissectPair = it.next();
                                    key = dissectPair.getKey();
                                    //add the key with an empty value for the empty delimiter
                                    dissectMatch.add(key, "");
                                }
                            } else {
                                break; //the while loop
                            }
                        }
                        //progress the keys/delimiter if possible
                        if (!it.hasNext()) {
                            break; //the for loop
                        }
                        dissectPair = it.next();
                        key = dissectPair.getKey();
                        delimiter = dissectPair.getDelimiter().getBytes(StandardCharsets.UTF_8);
                        //i is always one byte after the last found delimiter, aka the start of the next value
                        valueStart = i;
                    }
                }
            }
            //the last key, grab the rest of the input (unless consecutive delimiters already grabbed the last key)
            //and there is no trailing delimiter
            if (!dissectMatch.fullyMatched() && delimiter.length == 0 ) {
                byte[] value = Arrays.copyOfRange(input, valueStart, input.length);
                String valueString = new String(value, StandardCharsets.UTF_8);
                dissectMatch.add(key, valueString);
            }
        }
        Map<String, String> results = dissectMatch.getResults();

        if (!dissectMatch.isValid(results)) {
            throw new DissectException.FindMatch(pattern, inputString);
        }
        return results;
    }

    /**
     * A tuple class to hold the dissect key and delimiter
     */
    private class DissectPair {

        private final DissectKey key;
        private final String delimiter;

        private DissectPair(DissectKey key, String delimiter) {
            this.key = key;
            this.delimiter = delimiter;
        }

        private DissectKey getKey() {
            return key;
        }

        private String getDelimiter() {
            return delimiter;
        }
    }

}



