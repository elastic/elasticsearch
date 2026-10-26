/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;

/**
 * THE generic workload IT: one class enumerating the whole catalog via its
 * {@link ParametersFactory}. This single class is the extensibility contract — adding a corpus is
 * a catalog entry plus a csv-spec, and it is reachable from here with no new Java. Per-corpus
 * classes only ever appear if a corpus needs independent Buildkite sharding, and even then as a
 * 5-line subclass declaring a filter, not a new runner. A unit test asserts every workload corpus
 * in the catalog is reachable from these parameters, so a new corpus cannot be silently unrun.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class PublicDataIT extends PublicDataSpecTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = PublicDataClusters.shared();

    public PublicDataIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        VariantSpec variant
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, variant);
    }

    // Named readScriptSpec to HIDE the base class's @ParametersFactory of the same name: inherited
    // factories also run, and the base one feeds classpath-wide 6-arg csv-spec tuples into this
    // class's 7-arg constructor (the established idiom across the EsqlSpecTestCase subclasses).
    @ParametersFactory(argumentFormatting = "public-data:%2$s.%3$s{%7$s}")
    public static List<Object[]> readScriptSpec() throws Exception {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        List<Object[]> parameters = new ArrayList<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.kind() == CorpusSpec.Kind.WORKLOAD) {
                parameters.addAll(readScriptSpecWithVariants(catalog, corpus));
            }
        }
        // an empty filter result fails loudly, listing the available labels; a silent zero-test
        // run is the classic failure mode of filtered suites. Exception: filters that select only
        // failure-only corpora legitimately enumerate zero workload tests here — their cases run
        // in the sibling PublicDataFailureIT.
        PublicDataFilters filters = PublicDataFilters.fromSystemProperties();
        boolean failureOnlySelected = parameters.isEmpty()
            && catalog.corpora()
                .stream()
                .filter(filters::matches)
                .anyMatch(
                    corpus -> corpus.kind() == CorpusSpec.Kind.FAILURE_ONLY
                        && corpus.variants().stream().anyMatch(v -> v.expectFailure() != null && v.active() && filters.matches(v))
                );
        if (failureOnlySelected == false) {
            filters.failIfEmpty(parameters, catalog);
        }
        return parameters;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
