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
 */
public abstract class EsqlCsvSpecTestsExtension {

    /** Directory containing {@code *.csv-spec} resource files. */
    public abstract DirectoryProperty getSpecFilesDir();

    /** Java package for the generated test classes (e.g. {@code org.elasticsearch.xpack.esql.qa.single_node}). */
    public abstract Property<String> getPackageName();

    private final List<String> variantPrefixes = new ArrayList<>();
    private final List<String> variantBaseClasses = new ArrayList<>();

    /**
     * Registers a generated test variant.  For every {@code *.csv-spec} file found in
     * {@link #getSpecFilesDir()} the plugin will generate a class named
     * {@code <classPrefix><PascalSpecName>IT} that extends {@code baseClassName}.
     *
     * @param classPrefix   prefix for generated class names (e.g. {@code "EsqlSpec"})
     * @param baseClassName simple name of the hand-written abstract base class
     *                      (e.g. {@code "AbstractEsqlSpecIT"})
     */
    public void variant(String classPrefix, String baseClassName) {
        variantPrefixes.add(classPrefix);
        variantBaseClasses.add(baseClassName);
    }

    /** Returns the class-name prefixes for all registered variants, in declaration order. */
    List<String> getVariantPrefixes() {
        return Collections.unmodifiableList(variantPrefixes);
    }

    /** Returns the base-class names for all registered variants, in declaration order. */
    List<String> getVariantBaseClasses() {
        return Collections.unmodifiableList(variantBaseClasses);
    }
}
