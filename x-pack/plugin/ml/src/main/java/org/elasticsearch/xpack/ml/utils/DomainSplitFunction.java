/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

public final class DomainSplitFunction {

    private static final DeprecationLogger deprecationLogger =
        new DeprecationLogger(LogManager.getLogger(DomainSplitFunction.class));

    private static final int MAX_DOMAIN_PART_LENGTH = 63;

    private static final Map<String, String> exact;
    private static final Map<String, String> under;
    private static final Map<String, String> excluded;
    static {
        Map<String, String> exactMap = new HashMap<>(2048);

        String exactResourceName = "org/elasticsearch/xpack/ml/transforms/exact.properties";

        try (InputStream resource = DomainSplitFunction.class.getClassLoader().getResourceAsStream(exactResourceName)) {
            List<String> lines = Streams.readAllLines(resource);
            for (String line : lines) {
                String[] split = line.split("=");
                exactMap.put(split[0].trim(), split[1].trim());
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not load DomainSplit resource", e);
        }
        exact = Collections.unmodifiableMap(exactMap);

        Map<String, String> underMap = new HashMap<>(30);
        underMap.put("bd", "i");
        underMap.put("np", "i");
        underMap.put("jm", "i");
        underMap.put("fj", "i");
        underMap.put("fk", "i");
        underMap.put("ye", "i");
        underMap.put("sch.uk", "i");
        underMap.put("bn", "i");
        underMap.put("kitakyushu.jp", "i");
        underMap.put("kobe.jp", "i");
        underMap.put("ke", "i");
        underMap.put("sapporo.jp", "i");
        underMap.put("kh", "i");
        underMap.put("mm", "i");
        underMap.put("il", "i");
        underMap.put("yokohama.jp", "i");
        underMap.put("ck", "i");
        underMap.put("nagoya.jp", "i");
        underMap.put("sendai.jp", "i");
        underMap.put("kw", "i");
        underMap.put("er", "i");
        underMap.put("mz", "i");
        underMap.put("platform.sh", "p");
        underMap.put("gu", "i");
        underMap.put("nom.br", "i");
        underMap.put("zm", "i");
        underMap.put("pg", "i");
        underMap.put("ni", "i");
        underMap.put("kawasaki.jp", "i");
        underMap.put("zw", "i");
        under = Collections.unmodifiableMap(underMap);

        Map<String, String> excludedMap = new HashMap<>(9);
        excludedMap.put("city.yokohama.jp", "i");
        excludedMap.put("teledata.mz", "i");
        excludedMap.put("city.kobe.jp", "i");
        excludedMap.put("city.sapporo.jp", "i");
        excludedMap.put("city.kawasaki.jp", "i");
        excludedMap.put("city.nagoya.jp", "i");
        excludedMap.put("www.ck", "i");
        excludedMap.put("city.sendai.jp", "i");
        excludedMap.put("city.kitakyushu.jp", "i");
        excluded = Collections.unmodifiableMap(excludedMap);
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
            String [] pieces = ancestorName.split("\\.");
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
        if (!(publicSuffixIndex > 0)) {
            throw new IllegalArgumentException("Not under a public suffix: " + name);
        }
        return ancestor(parts, publicSuffixIndex - 1);
    }

    public static List<String> domainSplit(String host, Map<String, Object> params) {
        // NOTE: we don't check SpecialPermission because this will be called (indirectly) from scripts
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            deprecationLogger.deprecatedAndMaybeLog("domainSplit",
                "Method [domainSplit] taking params is deprecated. Remove the params argument.");
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
        for(int i = 0; i < host.length(); i++) {
            if (!(Character.isDigit(host.charAt(i)) || host.charAt(i) == '.')) {
                tentativeIP = false;
            break;
            }
        }
        if (tentativeIP) {
            /* special-snowflake rules now... */
            if (host.equals(".")) {
                return Arrays.asList("","");
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
            if (!parts.isEmpty()) {
                if (parts.size() == 1) {
                    return Arrays.asList("", host);
                }
                if (parts.size() > 2) {
                    boolean allNumeric = true;
                    String value = parts.get(parts.size() - 1);
                    for (int i = 0; i < value.length(); i++) {
                        if (!Character.isDigit(value.charAt(i))) {
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
