/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PatternedTextValueProcessor {
    private static final String TEXT_ARG_PLACEHOLDER = "%W";
    private static final String DELIMITER = "[\\s\\[\\]]";
    private static final String SPACE = " ";

    record Parts(String template, List<String> args, String indexed) {
        String templateStripped() {
            List<String> stripped = new ArrayList<>();
            String[] parts = template.split(SPACE);
            for (String part : parts) {
                if (part.startsWith("%") == false) {
                    stripped.add(part);
                }
            }
            return String.join(SPACE, stripped);
        }
    }

    static Parts split(String text) {
        StringBuilder template = new StringBuilder();
        StringBuilder indexed = new StringBuilder();
        List<String> args = new ArrayList<>();
        String[] tokens = text.split(DELIMITER);
        int textIndex = 0;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.isEmpty()) {
                if (textIndex < text.length() - 1) {
                    template.append(text.charAt(textIndex++));
                }
                continue;
            }
            if (isArg(token)) {
                args.add(token);
                template.append(TEXT_ARG_PLACEHOLDER);
                indexed.append(token).append(SPACE);
            } else {
                template.append(token);
                indexed.append(token).append(SPACE);
            }
            textIndex += token.length();
            if (textIndex < text.length()) {
                template.append(text.charAt(textIndex++));
            }
        }
        while (textIndex < text.length()) {
            template.append(text.charAt(textIndex++));
        }
        return new Parts(template.toString(), args, indexed.toString().trim());
    }

    private static boolean isArg(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isDigit(text.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static String merge(Parts parts) {
        StringBuilder builder = new StringBuilder();
        String[] templateParts = parts.template.split(DELIMITER);
        int i = 0;
        int templateIndex = 0;
        for (String part : templateParts) {
            if (part.equals(TEXT_ARG_PLACEHOLDER)) {
                builder.append(parts.args.get(i++));
                templateIndex += TEXT_ARG_PLACEHOLDER.length();
            } else if (part.isEmpty() == false) {
                builder.append(part);
                templateIndex += part.length();
            }
            if (templateIndex < parts.template.length()) {
                builder.append(parts.template.charAt(templateIndex++));
            }
        }
        assert i == parts.args.size() : "expected " + i + " but got " + parts.args.size();
        assert builder.toString().contains(TEXT_ARG_PLACEHOLDER) == false : builder.toString();
        while (templateIndex < parts.template.length()) {
            builder.append(parts.template.charAt(templateIndex++));
        }
        return builder.toString();
    }

    static String mergeRemainingArgs(Parts parts, int startOffset) {
        StringBuilder builder = new StringBuilder();
        for (int i = startOffset; i < parts.args.size(); i++) {
            builder.append((i > startOffset) ? SPACE : "").append(parts.args.get(i));
        }
        return builder.toString();
    }

    static void addRemainingArgs(List<String> args, String mergedArgs) {
        Collections.addAll(args, mergedArgs.split(SPACE));
    }

    static int countArgs(String template) {
        int count = 0;
        for (int i = 0; i < template.length() - 1; i++) {
            if (template.charAt(i) == '%') {
                char next = template.charAt(i + 1);
                if (next == 'W') {
                    count++;
                    i++;
                }
            }
        }
        return count;
    }
}
