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

    /** Naming convention that pairs a multi-source test with the single-dataset test it mirrors. */
    private static final String MULTI_SUFFIX = "Multi";

    public void testShippedCatalogAndSpecsAreValid() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        assertThat(CatalogValidator.validate(catalog, shippedWorkloads(catalog)), empty());
    }

    /**
     * The multi-source cross-check, enforced offline: a test named {@code <base>Multi} reads the
     * same rows as {@code <base>}, just through N datasets unioned by {@code FROM d1, ..., dN}
     * instead of one. Its expected table must therefore be identical — that identity is the whole
     * point of the pairing, and asserting it here means a copy-paste drift in either table is
     * caught on {@code check} rather than by a red nightly leg.
     */
    public void testMultiSourceTwinsCarryTheSameExpectedTable() {
        PublicDataCatalog catalog = PublicDataCatalog.loadFromClasspath(PublicDataCatalog.CATALOG_RESOURCE);
        int pairs = 0;
        for (CorpusSpec corpus : catalog.corpora()) {
            if (corpus.workload() == null) {
                continue;
            }
            WorkloadSpec workload = WorkloadSpec.loadFromClasspath(corpus.workload());
            Map<String, WorkloadSpec.TestSpec> byName = new LinkedHashMap<>();
            workload.tests().forEach(t -> byName.put(t.baseName(), t));
            for (WorkloadSpec.TestSpec test : workload.tests()) {
                if (test.baseName().endsWith(MULTI_SUFFIX) == false) {
                    continue;
                }
                String twinName = test.baseName().substring(0, test.baseName().length() - MULTI_SUFFIX.length());
                WorkloadSpec.TestSpec twin = byName.get(twinName);
                assertNotNull(
                    "multi-source test ["
                        + test.baseName()
                        + "] in ["
                        + corpus.workload()
                        + "] has no single-source twin ["
                        + twinName
                        + "]",
                    twin
                );
                assertTrue(
                    "multi-source test [" + test.baseName() + "] must bind more than one dataset",
                    test.datasetDirectives().size() > 1
                );
                assertEquals(
                    "multi-source test [" + test.baseName() + "] must return exactly what its twin [" + twinName + "] returns",
                    twin.expectedTable(),
                    test.expectedTable()
                );
                pairs++;
            }
        }
        logger.info("verified {} multi-source/single-source expected-table pairs", pairs);
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
