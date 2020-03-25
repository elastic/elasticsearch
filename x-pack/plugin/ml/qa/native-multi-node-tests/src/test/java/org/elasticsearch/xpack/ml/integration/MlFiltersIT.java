/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import static org.hamcrest.Matchers.equalTo;

public class MlFiltersIT extends MlNativeIntegTestCase {

    @Override
    protected void cleanUpResources() {
        // nothing to clean
    }

    public void testGetFilters_ShouldReturnUpTo100ByDefault() {
        int filtersCount = randomIntBetween(11, 100);
        for (int i = 0; i < filtersCount; i++) {
            putMlFilter(MlFilter.builder("filter-" + i).setItems("item-" + i).build());
        }

        GetFiltersAction.Response filters = getMlFilters();
        assertThat((int) filters.getFilters().count(), equalTo(filtersCount));
        assertThat(filters.getFilters().results().size(), equalTo(filtersCount));
    }
}
