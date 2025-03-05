/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocsV3Support {

    public static final DocsV3Support FUNCTIONS = new DocsV3Support("functions");

    public static final DocsV3Support OPERATORS = new DocsV3Support("operators");

    private static final Map<String, String> MACROS = new HashMap<>();
    private final String category;

    static {
        MACROS.put("wikipedia", "https://en.wikipedia.org/wiki");
        MACROS.put("javadoc", "https://docs.oracle.com/en/java/javase/11/docs/api");
    }

    private DocsV3Support(String category) {
        this.category = category;
    }

    public String replaceLinks(String text) {
        return replaceAsciidocLinks(replaceMacros(text));
    }

    private String replaceAsciidocLinks(String text) {
        Pattern pattern = Pattern.compile("<<([^>]*)>>");
        Matcher matcher = pattern.matcher(text);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String match = matcher.group(1);
            matcher.appendReplacement(result, getLink(match));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String replaceMacros(String text) {
        Pattern pattern = Pattern.compile("\\{([^}]+)}(/[^\\[]+)\\[([^]]+)\\]");

        Matcher matcher = pattern.matcher(text);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String macro = matcher.group(1);
            String path = matcher.group(2);
            String display = matcher.group(3);
            matcher.appendReplacement(result, getMacroLink(macro, path, display));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String getMacroLink(String macro, String path, String display) {
        if (MACROS.containsKey(macro) == false) {
            throw new IllegalArgumentException("Unknown macro [" + macro + "]");
        }
        return String.format(Locale.ROOT, "[%s](%s%s)", display, MACROS.get(macro), path);
    }

    private String getLink(String key) {
        if (key.startsWith("esql-")) {
            String displayText = key.substring("esql-".length()).toLowerCase(Locale.ROOT);
            int comma = displayText.indexOf(',');
            if (comma > 0) {
                key = "esql-" + displayText.substring(0, comma);
                displayText = displayText.substring(comma + 1).trim();
            } else {
                displayText = "`" + displayText.toUpperCase(Locale.ROOT) + "`";
            }
            return String.format(Locale.ROOT, "[%s](../../../esql-functions-operators.md#%s)", displayText, key);
        }
        throw new IllegalArgumentException("Invalid link key <<" + key + ">>");
    }
}
