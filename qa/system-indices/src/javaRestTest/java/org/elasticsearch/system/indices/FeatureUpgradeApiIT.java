/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.system.indices;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.XContentTestUtils;
import org.junit.After;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class FeatureUpgradeApiIT extends AbstractSystemIndicesIT {

    @After
    public void resetFeatures() throws Exception {
        client().performRequest(new Request("POST", "/_features/_reset"));
    }

    public void testCreatingSystemIndex() throws Exception {
        var request = new Request("PUT", "/_net_new_sys_index/_create");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    @SuppressWarnings("unchecked")
    public void testGetFeatureUpgradedStatuses() throws Exception {
        var request = new Request("PUT", "/_net_new_sys_index/_create");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
        client().performRequest(request);
        Response response = client().performRequest(new Request("GET", "/_migration/system_features"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        XContentTestUtils.JsonMapView view = XContentTestUtils.createJsonMapView(response.getEntity().getContent());
        String upgradeStatus = view.get("migration_status");
        assertThat(upgradeStatus, equalTo("NO_MIGRATION_NEEDED"));
        List<Map<String, Object>> features = view.get("features");
        Map<String, Object> testFeature = features.stream()
            .filter(feature -> "system indices qa".equals(feature.get("feature_name")))
            .findFirst()
            .orElse(Collections.emptyMap());

        assertThat(testFeature.size(), equalTo(4));
        assertThat(testFeature.get("minimum_index_version"), equalTo(IndexVersion.current().toReleaseVersion()));
        assertThat(testFeature.get("migration_status"), equalTo("NO_MIGRATION_NEEDED"));
        assertThat(testFeature.get("indices"), instanceOf(List.class));

        assertThat((List<Object>) testFeature.get("indices"), hasSize(1));
    }
}
