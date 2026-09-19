/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

/**
 * Generated-vector suite for ORC. Configuration comes from the declaration, not from this file.
 *
 * <p>ORC needs no fixture trees of its own. Every fixture-bound dimension is scoped away from it --
 * text_codec applies to the text formats, parquet_codec to parquet, and the dialect slots to csv and tsv --
 * so an ORC vector never carries a fixture slot off its default, {@code vectorReaderName} resolves to the
 * plain {@code orc} reader, and the standard fixtures serve every vector. What varies is the
 * format-agnostic axes: schema mode and resolution, path shape, partition detection, error mode,
 * distribution, and whether the caches are on.
 *
 * <p>That narrowness is the reason ORC had no vector coverage at all rather than a reason it could not:
 * the declaration called it a rule ({@code format.rule.orc}) on the grounds that ORC vector suites were
 * out of scope by decision, while recording in the same breath that its fixtures already existed and
 * nothing selected them. A decision is not an impossibility, and this is what selecting them looks like.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class OrcVectorSpecIT extends AbstractExternalSourceSpecTestCase {

    public static ElasticsearchCluster cluster = Clusters.testCluster(() -> s3Fixture.getAddress());

    @ClassRule
    public static TestRule ruleChain = chainFixturesBeforeCluster(cluster);

    private final Map<String, String> vector;
    private final Map<String, String> vectorSettings;

    public OrcVectorSpecIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        String vectorName,
        StorageBackend storageBackend
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, vectorReaderName("orc", vectorName));
        this.vector = FixtureDimensions.get().parseRendered(vectorName);
        this.vectorSettings = FixtureDimensions.get().directiveSettings(this.vector);
    }

    /** This suite routes its own spec set, so its exclusions are declared under its own token. */
    @Override
    protected String exclusionSuiteToken() {
        return "orc-vector";
    }

    @Override
    protected Map<String, String> vectorSettings() {
        return vectorSettings;
    }

    /**
     * The vector itself, so an exclusion can name the configurations it applies to. Without this the skip
     * path sees an empty vector and a {@code @dimension.value} exclusion never matches.
     */
    @Override
    protected Map<String, String> vector() {
        return vector;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s [%7$s/%8$s]")
    public static List<Object[]> readScriptSpec() throws Exception {
        return readExternalSpecTestsWithVectorsForSuite("orc", "orc-vector");
    }
}
