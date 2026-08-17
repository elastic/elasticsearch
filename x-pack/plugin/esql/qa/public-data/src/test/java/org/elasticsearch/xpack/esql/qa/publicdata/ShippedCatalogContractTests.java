/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CatalogValidator;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.CorpusSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.VariantSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;

/**
 * The extensibility contract over the SHIPPED catalog and specs: every workload corpus must be
 * reachable from {@code PublicDataIT}'s parameter enumeration (same catalog + same default
 * filters), so a new corpus can never be silently unrun; and the shipped resources must pass the
 * full structural validation — the same single source of truth the {@code
 * validatePublicDataCatalog} Gradle task runs.
 */
public class ShippedCatalogContractTests extends ESTestCase {

    public void testShippedCatalogAndSpecsAreValid() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        assertThat(CatalogValidator.validate(catalog, shippedWorkloads(catalog)), empty());
    }

    public void testEveryWorkloadCorpusIsReachableFromTheGenericIT() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        PublicDataFilters defaultFilters = new PublicDataFilters(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            0,
            3,
            "build/public-data-results",
            "8g"
        );
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.kind() != CorpusSpec.Kind.WORKLOAD) {
                continue;
            }
            List<VariantSpec> variants = defaultFilters.variants(corpus);
            assertFalse(
                "corpus [" + corpus.id() + "] has no active variant; it would be silently unrun by PublicDataIT",
                variants.isEmpty()
            );
            WorkloadSpec workload = WorkloadSpec.loadFromClasspath(corpus.workload());
            assertFalse("corpus [" + corpus.id() + "] workload [" + corpus.workload() + "] has no tests", workload.tests().isEmpty());
            for (VariantSpec variant : variants) {
                long effective = workload.tests()
                    .stream()
                    .filter(t -> variant.querySubset().isEmpty() || variant.querySubset().contains(t.baseName()))
                    .filter(t -> t.disabled() == false)
                    .count();
                assertTrue("variant [" + variant.label() + "] enumerates zero enabled tests", effective > 0);
            }
        }
    }

    private static Map<String, WorkloadSpec> shippedWorkloads(PublicDataCatalog catalog) {
        Map<String, WorkloadSpec> workloads = new LinkedHashMap<>();
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() != null) {
                workloads.put(corpus.workload(), WorkloadSpec.loadFromClasspath(corpus.workload()));
            }
        }
        return workloads;
    }
}
