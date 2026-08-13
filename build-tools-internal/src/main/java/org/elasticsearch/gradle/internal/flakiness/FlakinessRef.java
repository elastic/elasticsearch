/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.flakiness;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One heterogeneous input reference from {@code flakiness-refs.json} (contract 1), as produced by the
 * TypeScript bootstrap step. Refs are deliberately untyped-per-source: a {@code changed-file} ref carries
 * a repo-relative {@code path}; an {@code unmute} ref carries a {@code className} (and optional
 * {@code method}); an {@code explicit} ref carries a {@code spec} string. Exactly the fields relevant to
 * the {@code source} discriminator are populated; the rest are {@code null}.
 *
 * @param source    the ref origin: {@code "changed-file"}, {@code "unmute"}, or {@code "explicit"}
 * @param path      repo-relative file path (changed-file refs)
 * @param className fully-qualified class name (unmute refs)
 * @param method    optional JUnit method descriptor, e.g. {@code test {yaml=...}} (unmute refs)
 * @param spec      developer-supplied spec string, e.g. {@code org.foo.BarTests.testX} (explicit refs)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FlakinessRef(String source, String path, String className, String method, String spec) {

    public static final String SOURCE_CHANGED_FILE = "changed-file";
    public static final String SOURCE_UNMUTE = "unmute";
    public static final String SOURCE_EXPLICIT = "explicit";
}
