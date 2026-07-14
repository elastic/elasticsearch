/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Property;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * Configuration DSL for the {@code elasticsearch.esql-csv-spec-tests} plugin.
 *
 * <pre>{@code
 * esqlCsvSpecTests {
 *     specFilesDir = project(':...').file('src/main/resources')
 *     packageName  = 'org.elasticsearch.xpack.esql.qa.single_node'
 *     variant 'EsqlSpec', 'AbstractEsqlSpecIT'
 *     variant 'EsqlSpecForceStoredLoading', 'AbstractEsqlSpecForceStoredLoadingIT'
 * }
 * }</pre>
 *
 * <p>The plugin reads every {@code *.csv-spec} file from {@code specFilesDir} and, for
 * each declared {@link #variant}, generates one {@code <classPrefix><PascalName>IT.java}
 * class that extends the named {@code baseClassName}.  Each hand-written base class must
 * live in the same package inside the {@code csvSpecTest} source set.
 *
 * <p>An optional list of filename glob patterns can be supplied to restrict which spec files
 * a variant generates classes for.  When no patterns are given, the variant generates a class
 * for every {@code *.csv-spec} file found in {@code specFilesDir}.  When patterns are given,
 * only files whose name matches at least one pattern are included.
 *
 * <pre>{@code
 * esqlCsvSpecTests {
 *     specFilesDir = project(':...').file('src/main/resources')
 *     packageName  = 'org.elasticsearch.xpack.esql.qa.single_node'
 *     variant 'EsqlSpec', 'AbstractEsqlSpecIT'                         // all files
 *     variant 'EsqlSpec', 'AbstractEsqlSpecIT', 'external-*.csv-spec', 'parquet-*.csv-spec'  // filtered
 * }
 * }</pre>
 */
public abstract class EsqlCsvSpecTestsExtension {

    /** Directory containing {@code *.csv-spec} resource files. */
    public abstract DirectoryProperty getSpecFilesDir();

    /** Java package for the generated test classes (e.g. {@code org.elasticsearch.xpack.esql.qa.single_node}). */
    public abstract Property<String> getPackageName();

    private final List<String> variantPrefixes = new ArrayList<>();
    private final List<String> variantBaseClasses = new ArrayList<>();
    /**
     * Parallel to {@link #variantPrefixes}: comma-joined filename glob patterns per variant.
     * An empty string means "generate for all spec files".
     */
    private final List<String> variantSpecFilePatterns = new ArrayList<>();

    /**
     * Registers a generated test variant that generates classes for every {@code *.csv-spec}
     * file found in {@link #getSpecFilesDir()}.
     *
     * @param classPrefix   prefix for generated class names (e.g. {@code "EsqlSpec"})
     * @param baseClassName simple name of the hand-written abstract base class
     *                      (e.g. {@code "AbstractEsqlSpecIT"})
     */
    public void variant(String classPrefix, String baseClassName) {
        variantPrefixes.add(classPrefix);
        variantBaseClasses.add(baseClassName);
        variantSpecFilePatterns.add("");
    }

    /**
     * Registers a generated test variant restricted to spec files whose names match at
     * least one of the supplied filename glob patterns.  Patterns use the same syntax as
     * {@link java.nio.file.FileSystem#getPathMatcher} with the {@code glob:} prefix, applied
     * to the filename only (e.g. {@code "external-*.csv-spec"}).
     *
     * @param classPrefix       prefix for generated class names (e.g. {@code "EsqlSpec"})
     * @param baseClassName     simple name of the hand-written abstract base class
     * @param specFilePatterns  one or more filename glob patterns (e.g. {@code "external-*.csv-spec"})
     */
    public void variant(String classPrefix, String baseClassName, String... specFilePatterns) {
        variantPrefixes.add(classPrefix);
        variantBaseClasses.add(baseClassName);
        StringJoiner joiner = new StringJoiner(",");
        for (String pattern : specFilePatterns) {
            joiner.add(pattern);
        }
        variantSpecFilePatterns.add(joiner.toString());
    }

    /** Returns the class-name prefixes for all registered variants, in declaration order. */
    List<String> getVariantPrefixes() {
        return Collections.unmodifiableList(variantPrefixes);
    }

    /** Returns the base-class names for all registered variants, in declaration order. */
    List<String> getVariantBaseClasses() {
        return Collections.unmodifiableList(variantBaseClasses);
    }

    /**
     * Returns the per-variant filename glob patterns as comma-joined strings, parallel to
     * {@link #getVariantPrefixes()}.  An empty string means "generate for all spec files".
     */
    List<String> getVariantSpecFilePatterns() {
        return Collections.unmodifiableList(variantSpecFilePatterns);
    }
}
