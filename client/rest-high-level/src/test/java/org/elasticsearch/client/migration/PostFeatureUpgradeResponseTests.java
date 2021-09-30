/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.migration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PostFeatureUpgradeResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse, PostFeatureUpgradeResponse> {

    /** Our constructor should convert nulls to empty lists */
    public void testConstructorHandlesNullLists() {
        PostFeatureUpgradeResponse response = new PostFeatureUpgradeResponse(true, null, null, null);
        assertThat(response.getFeatures(), notNullValue());
        assertThat(response.getFeatures(), equalTo(Collections.emptyList()));
    }

    @Override
    protected org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse createServerTestInstance(
        XContentType xContentType) {
        if (randomBoolean()) {
            return new org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse(
                true,
                randomList(5,
                    () -> new org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse.Feature(
                        randomAlphaOfLengthBetween(5, 15)
                    )),
                null,
                null
            );
        } else {
            return new org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse(
                false,
                Collections.emptyList(),
                randomAlphaOfLengthBetween(10, 20),
                new ElasticsearchException(randomAlphaOfLengthBetween(10, 20)));
        }
    }

    @Override
    protected PostFeatureUpgradeResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return PostFeatureUpgradeResponse.parse(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse serverTestInstance,
        PostFeatureUpgradeResponse clientInstance) {

        assertThat(clientInstance.isAccepted(), equalTo(serverTestInstance.isAccepted()));

        assertThat(clientInstance.getFeatures(), hasSize(serverTestInstance.getFeatures().size()));

        for (int i = 0; i < clientInstance.getFeatures().size(); i++) {
            org.elasticsearch.action.admin.cluster.migration.PostFeatureUpgradeResponse.Feature serverFeature
                = serverTestInstance.getFeatures().get(i);
            PostFeatureUpgradeResponse.Feature clientFeature = clientInstance.getFeatures().get(i);

            assertThat(clientFeature.getFeatureName(), equalTo(serverFeature.getFeatureName()));
        }

        assertThat(clientInstance.getReason(), equalTo(serverTestInstance.getReason()));

        if (Objects.isNull(serverTestInstance.getElasticsearchException())) {
            assertThat(clientInstance.getElasticsearchException(), nullValue());
        } else {
            assertThat(clientInstance.getElasticsearchException(), notNullValue());
            assertThat(clientInstance.getElasticsearchException().getMessage(),
                containsString(serverTestInstance.getElasticsearchException().getMessage()));
        }
    }
}
