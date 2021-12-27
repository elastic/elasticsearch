/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public final class DomainSplitFunction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DomainSplitFunction.class);

    private static final int MAX_DOMAIN_PART_LENGTH = 63;

    private static final Map<String, String> exact;
    private static final Map<String, String> under = Map.ofEntries(
        entry("bd", "i"),
        entry("np", "i"),
        entry("jm", "i"),
        entry("fj", "i"),
        entry("fk", "i"),
        entry("ye", "i"),
        entry("sch.uk", "i"),
        entry("bn", "i"),
        entry("kitakyushu.jp", "i"),
        entry("kobe.jp", "i"),
        entry("ke", "i"),
        entry("sapporo.jp", "i"),
        entry("kh", "i"),
        entry("mm", "i"),
        entry("il", "i"),
        entry("yokohama.jp", "i"),
        entry("ck", "i"),
        entry("nagoya.jp", "i"),
        entry("sendai.jp", "i"),
        entry("kw", "i"),
        entry("er", "i"),
        entry("mz", "i"),
        entry("platform.sh", "p"),
        entry("gu", "i"),
        entry("nom.br", "i"),
        entry("zm", "i"),
        entry("pg", "i"),
        entry("ni", "i"),
        entry("kawasaki.jp", "i"),
        entry("zw", "i")
    );

    private static final Map<String, String> excluded = Map.of(
        "city.yokohama.jp",
        "i",
        "teledata.mz",
        "i",
        "city.kobe.jp",
        "i",
        "city.sapporo.jp",
        "i",
        "city.kawasaki.jp",
        "i",
        "city.nagoya.jp",
        "i",
        "www.ck",
        "i",
        "city.sendai.jp",
        "i",
        "city.kitakyushu.jp",
        "i"
    );

    static {
        try (
            var stream = DomainSplitFunction.class.getClassLoader()
                .getResourceAsStream("org/elasticsearch/xpack/ml/transforms/exact.properties")
        ) {
            exact = Streams.readAllLines(stream)
                .stream()
                .map(line -> line.split("="))
                .collect(Collectors.toUnmodifiableMap(split -> split[0], split -> split[1]));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DomainSplitFunction() {}

    private static String replaceDots(String input) {
        String output = input;
        if (output.indexOf('。') >= 0) {
            output = output.replace('。', '.');
        }
        if (output.indexOf('．') >= 0) {
            output = output.replace('．', '.');
        }
        if (output.indexOf('｡') >= 0) {
            output = output.replace('｡', '.');
        }
        return output;
    }

    private static List<String> splitDomain(String domain) {
        String dotDomain = replaceDots(domain);
        return Arrays.asList(dotDomain.split("\\."));
    }

    private static int findPublicSuffix(List<String> parts) {
        int partsSize = parts.size();
        for (int i = 0; i < partsSize; i++) {
            StringJoiner joiner = new StringJoiner(".");
            for (String s : parts.subList(i, partsSize)) {
                joiner.add(s);
            }
            /* parts.subList(i, partsSize).each(joiner::add); */
            String ancestorName = joiner.toString();
            if (exact.containsKey(ancestorName)) {
                return i;
            }
            /* Excluded domains (e.g. !nhs.uk) use the next highest
               domain as the effective public suffix (e.g. uk). */
            if (excluded.containsKey(ancestorName)) {
                return i + 1;
            }
            String[] pieces = ancestorName.split("\\.");
            if (pieces.length >= 2 && under.containsKey(pieces[1])) {
                return i;
            }
        }
        return -1;
    }

    private static String ancestor(List<String> parts, int levels) {
        StringJoiner joiner = new StringJoiner(".");
        for (String s : parts.subList(levels, parts.size())) {
            joiner.add(s);
        }
        String name = joiner.toString();
        if (name.endsWith(".")) {
            name = name.substring(0, name.length() - 1);
        }
        return name;
    }

    private static String topPrivateDomain(String name, List<String> parts, int publicSuffixIndex) {
        if (publicSuffixIndex == 1) {
            return name;
        }
        if (publicSuffixIndex <= 0) {
            throw new IllegalArgumentException("Not under a public suffix: " + name);
        }
        return ancestor(parts, publicSuffixIndex - 1);
    }

    public static List<String> domainSplit(String host, Map<String, Object> params) {
        // NOTE: we don't check SpecialPermission because this will be called (indirectly) from scripts
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "domainSplit",
                "Method [domainSplit] taking params is deprecated. Remove the params argument."
            );
            return null;
        });
        return domainSplit(host);
    }

    /**
     * Split {@code host} into sub domain and highest registered domain.
     * The result is a list containing exactly 2 items the first is the sub domain
     * and the second the highest registered domain.
     *
     * @param host The hostname to split
     * @return The sub domain and highest registered domain
     */
    public static List<String> domainSplit(String host) {
        host = host.trim();
        if (host.contains(":")) {
            return Arrays.asList("", host);
        }
        boolean tentativeIP = true;
        for (int i = 0; i < host.length(); i++) {
            if ((Character.isDigit(host.charAt(i)) || host.charAt(i) == '.') == false) {
                tentativeIP = false;
                break;
            }
        }
        if (tentativeIP) {
            /* special-snowflake rules now... */
            if (host.equals(".")) {
                return Arrays.asList("", "");
            }
            return Arrays.asList("", host);
        }
        String normalizedHost = host;
        normalizedHost = normalizedHost.toLowerCase(Locale.ROOT);
        List<String> parts = splitDomain(normalizedHost);
        int publicSuffixIndex = findPublicSuffix(parts);
        if (publicSuffixIndex == 0) {
            return Arrays.asList("", host);
        }
        String highestRegistered = "";
        /* for the case where the host is internal like .local so is not a recognised public suffix */
        if (publicSuffixIndex == -1) {
            if (parts.isEmpty() == false) {
                if (parts.size() == 1) {
                    return Arrays.asList("", host);
                }
                if (parts.size() > 2) {
                    boolean allNumeric = true;
                    String value = parts.get(parts.size() - 1);
                    for (int i = 0; i < value.length(); i++) {
                        if (Character.isDigit(value.charAt(i)) == false) {
                            allNumeric = false;
                            break;
                        }
                    }
                    if (allNumeric) {
                        highestRegistered = parts.get(parts.size() - 2) + '.' + parts.get(parts.size() - 1);
                    } else {
                        highestRegistered = parts.get(parts.size() - 1);
                    }

                } else {
                    highestRegistered = parts.get(parts.size() - 1);
                }
            }
        } else {
            /* HRD is the top private domain */
            highestRegistered = topPrivateDomain(normalizedHost, parts, publicSuffixIndex);
        }
        String subDomain = host.substring(0, host.length() - highestRegistered.length());
        if (subDomain.endsWith(".")) {
            subDomain = subDomain.substring(0, subDomain.length() - 1);
        }
        return Arrays.asList(subDomain, highestRegistered);
    }
}
