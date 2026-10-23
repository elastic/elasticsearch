/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import static org.hamcrest.Matchers.hasItem;

public class RestGetDatasetActionTests extends ESTestCase {

    /**
     * A declared rename is written under either spelling but always read back as {@code rename_from}, so a client has
     * to be able to discover the spelling of the RESPONSE from the read endpoint — the write endpoint's capability
     * says nothing about what GET returns. Pinned here because capability gating fails soft: a capability that stopped
     * being served makes a caller skip rather than fail, so nothing else turns dropping it into a red build.
     */
    public void testRenameFromCapabilityIsAdvertised() {
        assertThat(new RestGetDatasetAction().supportedCapabilities(), hasItem(EsqlDataSourcesCapabilities.DATASET_RENAME_FROM));
    }
}
