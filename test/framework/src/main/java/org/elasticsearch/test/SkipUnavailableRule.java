/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Test rule to process skip_unavailable override annotations
 */
public class SkipUnavailableRule implements TestRule {
    private final Map<String, Boolean> skipMap;

    public SkipUnavailableRule(String... clusterAliases) {
        this.skipMap = Arrays.stream(clusterAliases).collect(Collectors.toMap(Function.identity(), alias -> true));
    }

    public Map<String, Boolean> getMap() {
        return skipMap;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        // Check for annotation named "SkipOverride" and set the overrides accordingly
        var aliases = description.getAnnotation(NotSkipped.class);
        if (aliases != null) {
            for (String alias : aliases.aliases()) {
                skipMap.put(alias, false);
            }
        }
        return base;
    }

    /**
     * Annotation to mark specific cluster in a test as not to be skipped when unavailable
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface NotSkipped {
        String[] aliases();
    }

}
