/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.ndjson;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;

import java.util.List;
import java.util.Map;

/**
 * Generated-vector suite for ndjson. Configuration comes from the declaration, not from this file: the
 * vectors are whatever {@link FixtureDimensions#directiveExpressibleVectors} yields for this format, so a
 * value added to the declaration appears here with no edit.
 *
 * <p>The probes assert invariance on clean data -- see the spec header for what that does and does not
 * establish.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class NdJsonVectorSpecIT extends AbstractNdJsonExternalSpecTestCase {

    private final Map<String, String> vector;
    private final Map<String, String> vectorSettings;

    public NdJsonVectorSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String vectorName,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, vectorReaderName("ndjson", vectorName));
        this.vector = FixtureDimensions.get().parseRendered(vectorName);
        this.vectorSettings = FixtureDimensions.get().directiveSettings(this.vector);
    }

    /** This suite routes its own spec set, so its exclusions are declared under its own token. */
    @Override
    protected String exclusionSuiteToken() {
        return "ndjson-vector";
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
        return readExternalSpecTestsWithVectorsForSuite("ndjson", "ndjson-vector");
    }
}
