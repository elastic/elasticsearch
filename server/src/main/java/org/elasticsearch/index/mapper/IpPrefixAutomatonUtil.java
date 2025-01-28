/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static org.apache.lucene.util.automaton.Operations.concatenate;

/**
 * This class contains utility functionality to build an Automaton based
 * on a prefix String on an `ip` field.
 */
public class IpPrefixAutomatonUtil {

    private static final Automaton EMPTY_AUTOMATON = Automata.makeEmpty();
    private static final Automaton IPV4_PREFIX = Automata.makeBinary(new BytesRef(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 }));

    static final Map<Integer, Automaton> INCOMPLETE_IP4_GROUP_AUTOMATON_LOOKUP = new HashMap<>();
    static {
        for (int c = 0; c <= 255; c++) {
            Automaton a = Automata.makeChar(c);
            if (c > 0 && c < 10) {
                // all one digit prefixes expand to the two digit range, i.e. 1 -> [10..19]
                a = Operations.union(a, Automata.makeCharRange(c * 10, c * 10 + 9));
                // 1 and 2 even to three digit ranges
                if (c == 1) {
                    a = Operations.union(a, Automata.makeCharRange(100, 199));
                }
                if (c == 2) {
                    a = Operations.union(a, Automata.makeCharRange(200, 255));
                }
            }
            if (c >= 10 && c < 26) {
                int min = c * 10;
                int max = Math.min(c * 10 + 9, 255);
                a = Operations.union(a, Automata.makeCharRange(min, max));
            }
            INCOMPLETE_IP4_GROUP_AUTOMATON_LOOKUP.put(c, a);
        }
    }

    /**
     * Create a {@link CompiledAutomaton} from the ip Prefix.
     * If the prefix is empty, the automaton returned will accept everything.
     */
    static CompiledAutomaton buildIpPrefixAutomaton(String ipPrefix) {
        Automaton result;
        if (ipPrefix.isEmpty() == false) {
            Automaton ipv4Automaton = createIp4Automaton(ipPrefix);
            if (ipv4Automaton != null) {
                ipv4Automaton = concatenate(IPV4_PREFIX, ipv4Automaton);
            }
            Automaton ipv6Automaton = getIpv6Automaton(ipPrefix);
            result = Operations.union(ipv4Automaton, ipv6Automaton);
        } else {
            result = Automata.makeAnyBinary();
        }
        result = Operations.determinize(result, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        return new CompiledAutomaton(result, false, false, true);
    }

    private static Automaton getIpv6Automaton(String ipPrefix) {
        Automaton ipv6Automaton = EMPTY_AUTOMATON;
        List<String> ip6Groups = parseIp6Prefix(ipPrefix);
        if (ip6Groups.isEmpty() == false) {
            ipv6Automaton = Automata.makeString("");
            int groupsAdded = 0;
            for (String group : ip6Groups) {
                if (group.contains(".")) {
                    // try to parse this as ipv4 ending part, but only if we already have some ipv6 specific stuff in front
                    if (groupsAdded > 0) {
                        ipv6Automaton = concatenate(ipv6Automaton, createIp4Automaton(group));
                        groupsAdded += 2; // this counts as two bytes, missing bytes are padded already
                    } else {
                        return EMPTY_AUTOMATON;
                    }
                } else if (group.endsWith(":")) {
                    groupsAdded++;
                    // full block
                    if (group.length() > 1) {
                        group = group.substring(0, group.length() - 1);
                        ipv6Automaton = concatenate(ipv6Automaton, automatonFromIPv6Group(padWithZeros(group, 4 - group.length())));
                    } else {
                        // single colon denotes left out zeros
                        ipv6Automaton = concatenate(ipv6Automaton, Operations.repeat(Automata.makeChar(0)));
                    }
                } else {
                    // potentially partial block
                    if (groupsAdded == 0 && ONLY_ZEROS.matcher(group).matches()) {
                        // here we have a leading group with only "0" characters. If we allowed this to match
                        // ipv6 addresses, this would include things like 0000::127.0.0.1 (and all other ipv4 addresses).
                        // Allowing this would be counterintuitive, so "0*" prefixes should only expand
                        // to ipv4 addresses like "0.1.2.3" and we return with an automaton not matching anything here
                        return EMPTY_AUTOMATON;
                    }
                    // we need to create all possibilities of byte sequences this could match
                    groupsAdded++;
                    ipv6Automaton = concatenate(ipv6Automaton, automatonFromIPv6Group(group));
                }
            }
            // fill up the remainder of the 16 address bytes with wildcard matches, each group added so far counts for two bytes
            for (int i = 0; i < 16 - groupsAdded * 2; i++) {
                ipv6Automaton = concatenate(ipv6Automaton, Operations.optional(Automata.makeCharRange(0, 255)));
            }
        }
        return ipv6Automaton;
    }

    static Automaton automatonFromIPv6Group(String ipv6Group) {
        assert ipv6Group.length() > 0 && ipv6Group.length() <= 4 : "expected a full ipv6 group or prefix";
        Automaton result = Automata.makeEmpty();
        for (int leadingZeros = 0; leadingZeros <= 4 - ipv6Group.length(); leadingZeros++) {
            int bytesAdded = 0;
            String padded = padWithZeros(ipv6Group, leadingZeros);
            Automaton a = Automata.makeString("");
            while (padded.length() >= 2) {
                a = concatenate(a, Automata.makeChar(Integer.parseInt(padded.substring(0, 2), 16)));
                padded = padded.substring(2);
                bytesAdded++;
            }
            if (padded.length() == 1) {
                int value = Integer.parseInt(padded, 16);
                a = concatenate(a, Automata.makeCharRange(value * 16, value * 16 + 15));
                bytesAdded++;
            }
            if (bytesAdded != 2) {
                a = concatenate(a, Automata.makeCharRange(0, 255));
            }
            result = Operations.union(result, a);
        }
        return result;
    }

    private static Pattern IPV4_GROUP_MATCHER = Pattern.compile(
        "^((?:0|[1-9][0-9]{0,2})\\.)?" + "((?:0|[1-9][0-9]{0,2})\\.)?" + "((?:0|[1-9][0-9]{0,2})\\.)?" + "((?:0|[1-9][0-9]{0,2}))?$"
    );

    private static Pattern ONLY_ZEROS = Pattern.compile("^0+$");

    /**
     * Creates an {@link Automaton} that accepts all ipv4 address byte representation
     * that start with the given prefix. If the prefix is no valid ipv4 prefix, an automaton
     * that accepts the empty language is returned.
     */
    static Automaton createIp4Automaton(String prefix) {
        Matcher ip4Matcher = IPV4_GROUP_MATCHER.matcher(prefix);
        if (ip4Matcher.matches() == false) {
            return EMPTY_AUTOMATON;
        }
        int prefixBytes = 0;
        byte[] completeByteGroups = new byte[4];
        int completeBytes = 0;
        // scan the groups the prefix matches
        Automaton incompleteGroupAutomaton = Automata.makeString("");
        for (int g = 1; g <= 4; g++) {
            String group = ip4Matcher.group(g);
            // note that intermediate groups might not match anything and can be empty
            if (group != null) {
                if (group.endsWith(".")) {
                    // complete group found
                    int value = Integer.parseInt(group.substring(0, group.length() - 1));
                    if (value < 0 || value > 255) {
                        // invalid value, append the empty result to the current one to make it match nothing
                        return EMPTY_AUTOMATON;
                    } else {
                        completeByteGroups[completeBytes] = (byte) value;
                        completeBytes++;
                        prefixBytes++;
                    }
                } else {
                    // if present, this is the last group
                    int numberPrefix = Integer.parseInt(group);
                    if (numberPrefix <= 255) {
                        incompleteGroupAutomaton = INCOMPLETE_IP4_GROUP_AUTOMATON_LOOKUP.get(numberPrefix);
                        prefixBytes++;
                    } else {
                        // this cannot be a valid ip4 groups
                        return EMPTY_AUTOMATON;
                    }
                }
            }
        }
        return concatenate(
            List.of(
                Automata.makeBinary(new BytesRef(completeByteGroups, 0, completeBytes)),
                incompleteGroupAutomaton,
                Operations.repeat(Automata.makeCharRange(0, 255), 4 - prefixBytes, 4 - prefixBytes)
            )
        );
    }

    private static String padWithZeros(String input, int leadingZeros) {
        return new StringBuilder("0".repeat(leadingZeros)).append(input).toString();
    }

    private static Pattern IP6_BLOCK_MATCHER = Pattern.compile(
        "([a-f0-9]{0,4}:)|([a-f0-9]{1,4}$)" // the ipv6 specific notation
            + "|((?:(?:0|[1-9][0-9]{0,2})\\.){1,3}(?:0|[1-9][0-9]{0,2})?$)" // the optional ipv4 part
    );

    static List<String> parseIp6Prefix(String ip6Prefix) {
        Matcher ip6blockMatcher = IP6_BLOCK_MATCHER.matcher(ip6Prefix);
        int position = 0;
        List<String> groups = new ArrayList<>();
        while (ip6blockMatcher.find(position)) {
            if (ip6blockMatcher.start() == position) {
                position = ip6blockMatcher.end();
                IntStream.rangeClosed(1, 3).mapToObj(i -> ip6blockMatcher.group(i)).filter(s -> s != null).forEach(groups::add);
            } else {
                return Collections.emptyList();
            }
        }
        if (position != ip6Prefix.length()) {
            // no full match, return empty list
            return Collections.emptyList();
        }
        return groups;
    }
}
