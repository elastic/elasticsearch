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

import static java.util.Map.entry;

public class DocsV3Support {

    public static final DocsV3Support FUNCTIONS = new DocsV3Support("functions");

    public static final DocsV3Support OPERATORS = new DocsV3Support("operators");

    private static final Map<String, String> MACROS = new HashMap<>();
    private static final Map<String, String> knownFiles;

    static {
        MACROS.put("wikipedia", "https://en.wikipedia.org/wiki");
        MACROS.put("javadoc8", "https://docs.oracle.com/javase/8/docs/api");
        MACROS.put("javadoc14", "https://docs.oracle.com/en/java/javase/14/docs/api");
        MACROS.put("javadoc", "https://docs.oracle.com/en/java/javase/11/docs/api");
        // This static list of known root query-languages/esql/esql-*.md files is used to simulate old simple ascii-doc links
        knownFiles = Map.ofEntries(
            entry("esql-commands", "esql-commands.md"),
            entry("esql-enrich-data", "esql-enrich-data.md"),
            entry("esql-examples", "esql-examples.md"),
            entry("esql-functions-operators", "esql-functions-operators.md"),
            entry("esql-implicit-casting", "esql-implicit-casting.md"),
            entry("esql-metadata-fields", "esql-metadata-fields.md"),
            entry("esql-multivalued-fields", "esql-multivalued-fields.md"),
            entry("esql-process-data-with-dissect-grok", "esql-process-data-with-dissect-grok.md"),
            entry("esql-syntax", "esql-syntax.md"),
            entry("esql-time-spans", "esql-time-spans.md"),
            entry("esql-limitations", "limitations.md"),
            entry("esql-function-named-params", "esql-syntax.md")
        );
    }

    private final String category;

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

    /**
     * The new link syntax is extremely verbose.
     * Rather than make a large number of messy changes to the java files, we simply re-write the existing links to the new syntax.
     */
    private String getLink(String key) {
        // Known root esql markdown files
        String[] parts = key.split(",\\s*");
        if (knownFiles.containsKey(parts[0])) {
            return makeLink(key, "", "/reference/query-languages/esql/" + knownFiles.get(parts[0]));
        }
        // Old-style links within ES|QL reference
        if (key.startsWith("esql-")) {
            return makeLink(key, "esql-", "/reference/query-languages/esql/esql-functions-operators.md");
        }
        // Old-style links to Query DSL pages
        if (key.startsWith("query-dsl-")) {
            // <<query-dsl-match-query,match query>>
            // [`match`](/reference/query-languages/query-dsl-match-query.md)
            return makeLink(key, "query-dsl-", "/reference/query-languages/query-dsl-match-query.md");
        }
        // Various other remaining old asciidoc links
        // <<match-field-params,match query parameters>>
        return switch (parts[0]) {
            case "text" -> makeLink(key, "", "/reference/elasticsearch/mapping-reference/text.md");
            case "semantic-text" -> makeLink(key, "", "/reference/elasticsearch/mapping-reference/semantic-text.md");
            case "match-field-params" -> makeLink(key, "", "/reference/query-languages/query-dsl-match-query.md");
            default -> throw new IllegalArgumentException("Invalid link key <<" + key + ">>");
        };
    }

    private String makeLink(String key, String prefix, String parentFile) {
        String displayText = key.substring(prefix.length());
        int comma = displayText.indexOf(',');
        if (comma > 0) {
            key = prefix + displayText.substring(0, comma);
            displayText = displayText.substring(comma + 1).trim();
        } else {
            displayText = "`" + displayText.toUpperCase(Locale.ROOT) + "`";
        }
        if (parentFile.contains("/" + key + ".md")) {
            // The current docs-builder trips off all link targets that match the filename, so we need to do the same
            return String.format(Locale.ROOT, "[%s](%s)", displayText, parentFile);
        } else {
            return String.format(Locale.ROOT, "[%s](%s#%s)", displayText, parentFile, key);
        }
    }
}
