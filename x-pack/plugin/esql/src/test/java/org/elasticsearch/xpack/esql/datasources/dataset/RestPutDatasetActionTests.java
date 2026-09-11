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

public class RestPutDatasetActionTests extends ESTestCase {

    /**
     * The handler advertises the capability the yaml suite gates on. Pinned here because the yaml side fails soft: a
     * capability that stopped being served makes {@code requires} skip the section rather than fail it, so nothing
     * else turns dropping it into a red build.
     */
    public void testDeclaredTypeVocabularyCapabilityIsAdvertised() {
        assertThat(
            new RestPutDatasetAction().supportedCapabilities(),
            hasItem(EsqlDataSourcesCapabilities.DATASET_TEXT_TYPE_NOT_DECLARABLE)
        );
    }
}
