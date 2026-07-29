/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.test.ESTestCase;

import java.net.ConnectException;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.startsWith;

public class CsvTestsDataLoaderTests extends ESTestCase {

    public void testCsvTestsDataLoaderExecution() {
        ConnectException ce = expectThrows(ConnectException.class, () -> CsvTestsDataLoader.main(new String[] {}));
        assertThat(ce.getMessage(), startsWith("Connection refused"));
    }

    /**
     * The spec-data manifest (spec_data.yml) is the source of truth for index/enrich/inference/view
     * definitions. This asserts the manifest resource parses and populates the maps with the expected
     * entries (a smoke test that the resource is present, well-formed, and wired into the loader).
     * Regenerate the manifest with: loadCsvSpecData --args="--dump-manifest &lt;path&gt;/spec_data.yml".
     */
    public void testManifestPopulatesDefinitions() {
        assertThat(CsvTestsDataLoader.CSV_DATASET, hasKey("employees"));
        assertThat(CsvTestsDataLoader.CSV_DATASET, hasKey("languages_lookup"));
        assertThat(CsvTestsDataLoader.ENRICH_POLICIES, hasKey("languages_policy"));
        assertThat(CsvTestsDataLoader.INFERENCE_CONFIGS, hasKey("test_sparse_inference"));
        assertThat(CsvTestsDataLoader.VIEW_CONFIGS, hasKey("country_addresses"));
        // spot-check that non-default fields survive the round trip through the manifest
        CsvTestsDataLoader.TestDataset unmapped = CsvTestsDataLoader.CSV_DATASET.get("k8s_unmapped");
        assertEquals("false", unmapped.dynamic());
        assertTrue("expected removed field to be present as a null-valued type mapping", unmapped.typeMapping().containsKey("region"));
        assertNull(unmapped.typeMapping().get("region"));
    }
}
