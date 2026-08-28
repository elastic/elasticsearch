/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.csv;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureMatrix;

import java.util.List;
import java.util.Map;

/**
 * The first suite driven by generated dimension vectors rather than a hand-written configuration.
 *
 * <p>Each case runs once per vector that the {@code WITH} clause alone can express -- every off-default
 * slot binds as a directive and declares a key. The vector's name is a test parameter, so a failure says
 * which combination broke, and {@link #vectorSettings()} turns that name back into the settings injected
 * into every dataset directive.
 *
 * <p>The probes assert INVARIANCE: they read one clean file, on which none of the varied dimensions may
 * change the answer. What each dimension does when it matters -- a malformed row, files that disagree --
 * needs per-vector expectations, which a csv-spec case cannot carry. That is not covered here.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class CsvVectorSpecIT extends AbstractDelimitedTextSpecTestCase {

    private final Map<String, String> vector;
    private final Map<String, String> vectorSettings;

    public CsvVectorSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String vectorName,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, vectorReaderName("csv", vectorName));
        this.vector = FixtureDimensions.get().parseRendered(vectorName);
        this.vectorSettings = FixtureDimensions.get().directiveSettings(this.vector);
    }

    /**
     * This suite routes its own spec set, so its exclusions are declared under its own token. Without the
     * override the lookup falls back to csv and would apply another suite's exclusions.
     */
    @Override
    protected String exclusionSuiteToken() {
        return "csv-vector";
    }

    @Override
    protected Map<String, String> vectorSettings() {
        return vectorSettings;
    }

    /**
     * The vector itself, so an exclusion can name the configurations it applies to. Without this the
     * skip path sees an empty vector and a `@dimension.value` exclusion never matches -- which would
     * mean either excluding a case under every vector or not at all.
     */
    @Override
    protected Map<String, String> vector() {
        return vector;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithVectorsForSuite("csv", "csv-vector");
    }
}
