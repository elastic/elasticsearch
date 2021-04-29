/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public class MlFiltersIT extends MlSingleNodeTestCase {

    @Before
    public void beforeTests() throws Exception {
        waitForMlTemplates();
    }

    public void testGetFilters_ShouldReturnUpTo100ByDefault() {
        int filtersCount = randomIntBetween(11, 100);
        for (int i = 0; i < filtersCount; i++) {
            PutFilterAction.Request putFilterRequest = new PutFilterAction.Request(
                MlFilter.builder("filter-" + i).setItems("item-" + i).build());
            client().execute(PutFilterAction.INSTANCE, putFilterRequest).actionGet();
        }

        GetFiltersAction.Response filters = client().execute(GetFiltersAction.INSTANCE, new GetFiltersAction.Request()).actionGet();
        assertThat((int) filters.getFilters().count(), equalTo(filtersCount));
        assertThat(filters.getFilters().results().size(), equalTo(filtersCount));
    }
}
