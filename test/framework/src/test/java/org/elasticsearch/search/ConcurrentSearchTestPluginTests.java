/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Map;

public class ConcurrentSearchTestPluginTests extends ESIntegTestCase {

    public void testPlugin() {
        Map<String, Integer> settings = Map.of(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), randomIntBetween(1, 100));
        if (eagerConcurrentSearch()) {
            assertThat(SearchService.MINIMUM_DOCS_PER_SLICE.get(clusterService().getSettings()), Matchers.equalTo(1));
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                clusterAdmin().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
            });
            assertThat(
                ex.getMessage(),
                Matchers.containsString(
                    "transient setting [" + SearchService.MINIMUM_DOCS_PER_SLICE.getKey() + "], " + "not dynamically updateable"
                )
            );
        } else {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
                clusterAdmin().prepareUpdateSettings().setTransientSettings(settings).execute().actionGet();
            });
            assertThat(
                ex.getMessage(),
                Matchers.containsString("transient setting [" + SearchService.MINIMUM_DOCS_PER_SLICE.getKey() + "], not recognized")
            );
        }
    }
}
