/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;

import java.util.List;

/**
 * Configuration for loading a {@code flattened} field root with only some of its keys, so that a query that keeps
 * {@code attrs.b*} reads {@code attrs} with the keys that match {@code b*}. A key is loaded if it matches one of
 * {@code includes}, or {@code includes} is empty, and matches none of {@code excludes}. Patterns are simple wildcard
 * patterns over the dotted key, such as {@code "host.*"}.
 */
public record FlattenedKeysConfig(List<String> includes, List<String> excludes) implements BlockLoaderFunctionConfig {
    public FlattenedKeysConfig {
        includes = List.copyOf(includes);
        excludes = List.copyOf(excludes);
    }

    /**
     * Whether the root is loaded with {@code key}.
     */
    public boolean includes(String key) {
        if (includes.isEmpty() == false && includes.stream().noneMatch(pattern -> Regex.simpleMatch(pattern, key))) {
            return false;
        }
        return excludes.stream().noneMatch(pattern -> Regex.simpleMatch(pattern, key));
    }

    @Override
    public Function function() {
        return Function.FLATTENED_KEYS;
    }
}
