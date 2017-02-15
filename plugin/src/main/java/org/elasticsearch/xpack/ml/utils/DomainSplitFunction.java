/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

public final class DomainSplitFunction {

    public static final String function;
    public static final Map<String, Object> params;

    DomainSplitFunction() {}

    static {
        Map<String, Object> paramsMap = new HashMap<>();

        ResourceBundle resource = ResourceBundle.getBundle("org/elasticsearch/xpack/ml/transforms/exact", Locale.getDefault());
        Enumeration<String> keys = resource.getKeys();
        Map<String, String> exact = new HashMap<>(2048);
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            String value = resource.getString(key);
            exact.put(key, value);
        }
        exact = Collections.unmodifiableMap(exact);

        Map<String, Object> under = new HashMap<>(30);
        under.put("bd", "i");
        under.put("np", "i");
        under.put("jm", "i");
        under.put("fj", "i");
        under.put("fk", "i");
        under.put("ye", "i");
        under.put("sch.uk", "i");
        under.put("bn", "i");
        under.put("kitakyushu.jp", "i");
        under.put("kobe.jp", "i");
        under.put("ke", "i");
        under.put("sapporo.jp", "i");
        under.put("kh", "i");
        under.put("mm", "i");
        under.put("il", "i");
        under.put("yokohama.jp", "i");
        under.put("ck", "i");
        under.put("nagoya.jp", "i");
        under.put("sendai.jp", "i");
        under.put("kw", "i");
        under.put("er", "i");
        under.put("mz", "i");
        under.put("platform.sh", "p");
        under.put("gu", "i");
        under.put("nom.br", "i");
        under.put("zm", "i");
        under.put("pg", "i");
        under.put("ni", "i");
        under.put("kawasaki.jp", "i");
        under.put("zw", "i");
        under = Collections.unmodifiableMap(under);

        Map<String, String> excluded = new HashMap<>(9);
        excluded.put("city.yokohama.jp", "i");
        excluded.put("teledata.mz", "i");
        excluded.put("city.kobe.jp", "i");
        excluded.put("city.sapporo.jp", "i");
        excluded.put("city.kawasaki.jp", "i");
        excluded.put("city.nagoya.jp", "i");
        excluded.put("www.ck", "i");
        excluded.put("city.sendai.jp", "i");
        excluded.put("city.kitakyushu.jp", "i");
        excluded = Collections.unmodifiableMap(excluded);


        paramsMap.put("excluded", excluded);
        paramsMap.put("under", under);
        paramsMap.put("exact", exact);
        params = Collections.unmodifiableMap(paramsMap);
    }

    static {
        String fn = "String replaceDots(String input) {\n" +
                "    String output = input;\n" +
                "    if (output.indexOf('。') >= 0) {\n" +
                "        output = output.replace('。', '.');\n" +
                "    }\n" +
                "    if (output.indexOf('．') >= 0) {\n" +
                "        output = output.replace('．', '.');\n" +
                "    }\n" +
                "    if (output.indexOf('｡') >= 0) {\n" +
                "        output = output.replace('｡', '.');\n" +
                "    }\n" +
                "    return output;\n" +
                "}\n" +
                "List split(String value) {\n" +
                "    int nextWord = 0;\n" +
                "    List splits  = [];\n" +
                "    for(int i = 0; i < value.length(); i++) {\n" +
                "        if(value.charAt(i) == (char)'.') {\n" +
                "            splits.add(value.substring(nextWord, i));\n" +
                "            nextWord = i+1;\n" +
                "        }\n" +
                "    }\n" +
                "    if (nextWord != value.length()) {\n" +
                "        splits.add(value.substring(nextWord, value.length()));\n" +
                "    }\n" +
                "    return splits;\n" +
                "}\n" +
                "List splitDomain(String domain) {\n" +
                "    String dotDomain = replaceDots(domain);\n" +
                "    return split(dotDomain);\n" +
                "}\n" +
                "boolean validateSyntax(List parts) {\n" +
                "    int lastIndex = parts.length - 1;\n" +
                "    /* Validate the last part specially, as it has different syntax rules. */\n" +
                "    if (!validatePart(parts[lastIndex], true)) {\n" +
                "        return false;\n" +
                "    }\n" +
                "    for (int i = 0; i < lastIndex; i++) {\n" +
                "        String part = parts[i];\n" +
                "        if (!validatePart(part, false)) {\n" +
                "            return false;\n" +
                "        }\n" +
                "    }\n" +
                "    return true;\n" +
                "}\n" +
                "boolean validatePart(String part, boolean isFinalPart) {\n" +
                "    int MAX_DOMAIN_PART_LENGTH = 63;\n" +
                "    if (part.length() < 1 || part.length() > MAX_DOMAIN_PART_LENGTH) {\n" +
                "        return false;\n" +
                "    }\n" +
                "    int offset = 0;\n" +
                "    int strLen = part.length();\n" +
                "    while (offset < strLen) {\n" +
                "        int curChar = part.charAt(offset);\n" +
                "        offset += 1;\n" +
                "        if (!(Character.isLetterOrDigit(curChar) || curChar == (char)'-' || curChar == (char)'_')) {\n" +
                "            return false;\n" +
                "        }\n" +
                "    }\n" +
                "    if (part.charAt(0) == (char)'-' || part.charAt(0) == (char)'_' ||\n" +
                "            part.charAt(part.length() - 1) == (char)'-' || part.charAt(part.length() - 1) == (char)'_') {\n" +
                "        return false;\n" +
                "    }\n" +
                "    if (isFinalPart && Character.isDigit(part.charAt(0))) {\n" +
                "        return false;\n" +
                "    }\n" +
                "    return true;\n" +
                "}\n" +
                "int findPublicSuffix(Map params, List parts) {\n" +
                "   int partsSize = parts.size();\n" +
                "\n" +
                "   for (int i = 0; i < partsSize; i++) {\n" +
                "       StringJoiner joiner = new StringJoiner('.');\n" +
                "       for (String s : parts.subList(i, partsSize)) {\n" +
                "           joiner.add(s);\n" +
                "       }\n" +
                "       /* parts.subList(i, partsSize).each(joiner::add); */\n" +
                "       String ancestorName = joiner.toString();\n" +
                "\n" +
                "       if (params['exact'].containsKey(ancestorName)) {\n" +
                "           return i;\n" +
                "       }\n" +
                "\n" +
                "       /* Excluded domains (e.g. !nhs.uk) use the next highest\n" +
                "        domain as the effective public suffix (e.g. uk). */\n" +
                "\n" +
                "       if (params['excluded'].containsKey(ancestorName)) {\n" +
                "           return i + 1;\n" +
                "       }\n" +
                "\n" +
                "       List pieces = split(ancestorName);\n" +
                "       if (pieces.length >= 2 && params['under'].containsKey(pieces[1])) {\n" +
                "           return i;\n" +
                "       }\n" +
                "   }\n" +
                "\n" +
                "   return -1;\n" +
                "}\n" +
                "String ancestor(List parts, int levels) {\n" +
                "   StringJoiner joiner = new StringJoiner('.');\n" +
                "   for (String s : parts.subList(levels, parts.size())) {\n" +
                "      joiner.add(s);\n" +
                "   }\n" +
                "   String name = joiner.toString();\n" +
                "   if (name.endsWith('.')) {\n" +
                "       name = name.substring(0, name.length() - 1);\n" +
                "   }\n" +
                "   return name;\n" +
                "}\n" +
                "String topPrivateDomain(String name, List parts, int publicSuffixIndex) {\n" +
                "   if (publicSuffixIndex == 1) {\n" +
                "       return name;\n" +
                "   }\n" +
                "   if (!(publicSuffixIndex > 0)) {\n" +
                "       throw new IllegalArgumentException('Not under a public suffix: ' + name);\n" +
                "   }\n" +
                "   return ancestor(parts, publicSuffixIndex - 1);\n" +
                "}\n" +
                "List domainSplit(String host, Map params) {\n" +
                "    int MAX_DNS_NAME_LENGTH = 253;\n" +
                "    int MAX_LENGTH = 253;\n" +
                "    int MAX_PARTS = 127;\n" +
                "    if ('host'.isEmpty()) {\n" +
                "        return ['',''];\n" +
                "    }\n" +
                "    host = host.trim();\n" +
                "    if (host.contains(':')) {\n" +
                "        return ['', host];\n" +
                "    }\n" +
                "    boolean tentativeIP = true;\n" +
                "    for(int i = 0; i < host.length(); i++) {\n" +
                "        if (!(Character.isDigit(host.charAt(i)) || host.charAt(i) == (char)'.')) {\n" +
                "            tentativeIP = false;\n" +
                "            break;\n" +
                "        }\n" +
                "    }\n" +
                "    if (tentativeIP) {\n" +
                "        /* special-snowflake rules now... */\n" +
                "        if (host == '.') {\n" +
                "            return ['',''];\n" +
                "        }\n" +
                "        return ['', host];\n" +
                "    }\n" +
                "    def normalizedHost = host;\n" +
                "    normalizedHost = normalizedHost.toLowerCase();\n" +
                "    List parts = splitDomain(normalizedHost);\n" +
                "    int publicSuffixIndex = findPublicSuffix(params, parts);\n" +
                "    if (publicSuffixIndex == 0) {\n" +
                "        return ['', host];\n" +
                "    }\n" +
                "    String highestRegistered = '';\n" +
                "    /* for the case where the host is internal like .local so is not a recognised public suffix */\n" +
                "    if (publicSuffixIndex == -1) {\n" +
                "        if (!parts.isEmpty()) {\n" +
                "            if (parts.size() == 1) {\n" +
                "                return ['', host];\n" +
                "            }\n" +
                "            if (parts.size() > 2) {\n" +
                "                boolean allNumeric = true;\n" +
                "                String value = parts.get(parts.size() - 1);\n" +
                "                for (int i = 0; i < value.length(); i++) {\n" +
                "                    if (!Character.isDigit(value.charAt(i))) {\n" +
                "                        allNumeric = false;\n" +
                "                        break;\n" +
                "                    }\n" +
                "                }\n" +
                "                if (allNumeric) {\n" +
                "                    highestRegistered = parts.get(parts.size() - 2) + '.' + parts.get(parts.size() - 1);\n" +
                "                } else {\n" +
                "                    highestRegistered = parts.get(parts.size() - 1);\n" +
                "                }\n" +
                "\n" +
                "            } else {\n" +
                "                highestRegistered = parts.get(parts.size() - 1);\n" +
                "            }\n" +
                "        }\n" +
                "    } else {\n" +
                "        /* HRD is the top private domain */\n" +
                "        highestRegistered = topPrivateDomain(normalizedHost, parts, publicSuffixIndex);\n" +
                "    }\n" +
                "    String subDomain = host.substring(0, host.length() - highestRegistered.length());\n" +
                "    if (subDomain.endsWith('.')) {\n" +
                "        subDomain = subDomain.substring(0, subDomain.length() - 1);\n" +
                "    }\n" +
                "    return [subDomain, highestRegistered];\n" +
                "}\n";
        fn = fn.replace("\n"," ");
        function = fn;
    }
}
