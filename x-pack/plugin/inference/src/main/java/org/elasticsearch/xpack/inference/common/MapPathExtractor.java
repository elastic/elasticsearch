/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapPathExtractor {

    private static final String DOLLAR_DOT = "$.";
    private static final String DOLLAR = "$";

    public static Object extract(Map<String, Object> data, String path) {
        if (data == null || data.isEmpty() || path == null || path.trim().isEmpty()) {
            return null;
        }

        var cleanedPath = path.trim();

        // Remove the prefix if it exists
        if (cleanedPath.startsWith(DOLLAR_DOT)) {
            cleanedPath = cleanedPath.substring(DOLLAR_DOT.length());
        } else if (cleanedPath.startsWith(DOLLAR)) {
            cleanedPath = cleanedPath.substring(DOLLAR.length());
        }

        return navigate(data, cleanedPath);
    }

    private static Object navigate(Object current, String remainingPath) {
        if (remainingPath == null || remainingPath.isEmpty()) {
            return current;
        }

        var dotFieldPattern = Pattern.compile("^\\.([^.\\[]+)(.*)");
        // var arrayIndexPattern = Pattern.compile("^\\[(\\d+)\\](.*)");
        var arrayWildcardPattern = Pattern.compile("^\\[\\*\\](.*)");

        Matcher dotFieldMatcher = dotFieldPattern.matcher(remainingPath);
        // Matcher arrayIndexMatcher = arrayIndexPattern.matcher(remainingPath);
        Matcher arrayWildcardMatcher = arrayWildcardPattern.matcher(remainingPath);

        if (dotFieldMatcher.matches()) {
            String field = dotFieldMatcher.group(1);
            String nextPath = dotFieldMatcher.group(2);
            if (current instanceof Map) {
                return navigate(((Map<?, ?>) current).get(field), nextPath);
            }
        } else if (arrayIndexMatcher.matches()) {
            String indexStr = arrayIndexMatcher.group(1);
            String nextPath = arrayIndexMatcher.group(2);
            try {
                int index = Integer.parseInt(indexStr);
                if (current instanceof List) {
                    List<?> list = (List<?>) current;
                    if (index >= 0 && index < list.size()) {
                        return navigate(list.get(index), nextPath);
                    }
                }
            } catch (NumberFormatException e) {
                // Ignore invalid index
            }
        } else if (arrayWildcardMatcher.matches()) {
            String nextPath = arrayWildcardMatcher.group(1);
            if (current instanceof List) {
                List<?> list = (List<?>) current;
                List<Object> results = new ArrayList<>();
                for (Object item : list) {
                    Object result = navigate(item, nextPath);
                    if (result != null) {
                        if (result instanceof List) {
                            results.addAll((List<?>) result);
                        } else {
                            results.add(result);
                        }
                    }
                }
                return results.isEmpty() ? null : results;
            }
        }

        return null; // Path not found or invalid
    }
}
