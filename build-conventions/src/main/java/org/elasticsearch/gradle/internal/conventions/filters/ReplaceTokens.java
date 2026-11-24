/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.filters;

import java.io.Reader;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Copy filter that replaces {@code @key@} tokens with the configured values. Drop-in replacement for Ant's
 * {@code org.apache.tools.ant.filters.ReplaceTokens}, which won't be available on the build classpath in Gradle 10.
 *
 * <pre>
 * filter("tokens": [version: "1.0"], ReplaceTokens)
 * copySpec.filter(Map.of("tokens", tokens), ReplaceTokens.class)
 * </pre>
 *
 * <p>Semantics match Ant's implementation: at every position the input is checked for a configured {@code @key@}
 * token. Unknown tokens are left untouched and do not consume the closing {@code @}, so in {@code @unknown@known@}
 * the {@code @known@} token is still replaced. Replacement values are not scanned for further tokens. If several
 * tokens match at the same position the shortest one wins.
 */
public class ReplaceTokens extends BufferingFilterReader {

    static final String TOKEN_DELIMITER = "@";

    private Map<String, String> tokens = Map.of();

    public ReplaceTokens(Reader in) {
        super(in);
    }

    /**
     * Sets the tokens to replace, keyed by token name (without the surrounding {@code @}). Values are converted with
     * {@link String#valueOf(Object)}.
     */
    public void setTokens(Map<String, ?> tokens) {
        Map<String, String> resolved = new LinkedHashMap<>();
        tokens.forEach((key, value) -> resolved.put(TOKEN_DELIMITER + key + TOKEN_DELIMITER, String.valueOf(value)));
        this.tokens = resolved;
    }

    @Override
    protected String transform(String content) {
        return replace(content, tokens);
    }

    /**
     * @param tokens replacement values keyed by the full token including delimiters, e.g. {@code @version@}
     */
    static String replace(String content, Map<String, String> tokens) {
        if (tokens.isEmpty() || content.indexOf(TOKEN_DELIMITER.charAt(0)) < 0) {
            return content;
        }
        List<Map.Entry<String, String>> candidates = tokens.entrySet()
            .stream()
            .sorted(Comparator.comparingInt((Map.Entry<String, String> e) -> e.getKey().length()))
            .toList();
        StringBuilder result = new StringBuilder(content.length());
        int i = 0;
        while (i < content.length()) {
            Map.Entry<String, String> match = null;
            if (content.charAt(i) == TOKEN_DELIMITER.charAt(0)) {
                for (Map.Entry<String, String> candidate : candidates) {
                    if (content.startsWith(candidate.getKey(), i)) {
                        match = candidate;
                        break;
                    }
                }
            }
            if (match != null) {
                result.append(match.getValue());
                i += match.getKey().length();
            } else {
                result.append(content.charAt(i));
                i++;
            }
        }
        return result.toString();
    }
}
