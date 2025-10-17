/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.equalTo;

public class ElasticInferenceServiceUsageContextTests extends ESTestCase {

    public void testInputTypeToUsageContext_Search() {
        assertThat(
            ElasticInferenceServiceUsageContext.fromInputType(InputType.SEARCH),
            equalTo(ElasticInferenceServiceUsageContext.SEARCH)
        );
    }

    public void testInputTypeToUsageContext_Ingest() {
        assertThat(
            ElasticInferenceServiceUsageContext.fromInputType(InputType.INGEST),
            equalTo(ElasticInferenceServiceUsageContext.INGEST)
        );
    }

    public void testInputTypeToUsageContext_Unspecified() {
        assertThat(
            ElasticInferenceServiceUsageContext.fromInputType(InputType.UNSPECIFIED),
            equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED)
        );
    }

    public void testInputTypeToUsageContext_Unknown_DefaultToUnspecified() {
        assertThat(
            ElasticInferenceServiceUsageContext.fromInputType(InputType.CLASSIFICATION),
            equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED)
        );
        assertThat(
            ElasticInferenceServiceUsageContext.fromInputType(InputType.CLUSTERING),
            equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED)
        );
    }

    public void testProductUseCase() {
        assertThat(ElasticInferenceServiceUsageContext.SEARCH.productUseCaseHeaderValue(), Matchers.is("internal_search"));
        assertThat(ElasticInferenceServiceUsageContext.INGEST.productUseCaseHeaderValue(), Matchers.is("internal_ingest"));
        assertThat(ElasticInferenceServiceUsageContext.UNSPECIFIED.productUseCaseHeaderValue(), Matchers.is("unspecified"));
    }
}
