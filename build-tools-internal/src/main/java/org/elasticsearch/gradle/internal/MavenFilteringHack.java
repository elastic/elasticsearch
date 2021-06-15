/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.tools.ant.filters.ReplaceTokens;
import org.gradle.api.file.CopySpec;

/**
 * Gradle provides "expansion" functionality using groovy's SimpleTemplatingEngine (TODO: check name).
 * However, it allows substitutions of the form {@code $foo} (no curlies). Rest tests provide
 * some substitution from the test runner, which this form is used for.
 *
 * This class provides a helper to do maven filtering, where only the form {@code $\{foo\}} is supported.
 *
 * TODO: we should get rid of this hack, and make the rest tests use some other identifier
 * for builtin vars
 */
public class MavenFilteringHack {
    /**
     * Adds a filter to the given copy spec that will substitute maven variables.
     *
     */
    static void filter(CopySpec copySpec, Map<Object, Object> substitutions) {
        Map<String, String> mavenSubstitutions = new LinkedHashMap<>();
        Map<String, Object> argMap = new LinkedHashMap<>();

        substitutions.forEach((k, v) -> mavenSubstitutions.put("{" + k.toString(), v.toString()));

        argMap.put("tokens", mavenSubstitutions);
        argMap.put("beginToken", "$");
        argMap.put("endToken", "}");

        copySpec.filter(argMap, ReplaceTokens.class);
    }
}
