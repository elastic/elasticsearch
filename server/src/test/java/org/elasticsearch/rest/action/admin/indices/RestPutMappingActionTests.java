/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class RestPutMappingActionTests extends RestActionTestCase {

    private FakeRestRequest request(int padding) {
        String body = "{\"_meta\":{\"x\":\"" + "a".repeat(padding) + "\"}}";
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(org.elasticsearch.rest.RestRequest.Method.PUT)
            .withPath("/index/_mapping")
            .withParams(Map.of("index", "index"))
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }

    public void testLimitIsDynamic() throws Exception {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(RestPutMappingAction.MAX_MAPPING_REQUEST_SIZE_SETTING)
        );
        RestPutMappingAction action = new RestPutMappingAction(Settings.EMPTY, clusterSettings);
        assertNotNull(action.prepareRequest(request(100), verifyingClient));
        clusterSettings.applySettings(Settings.builder().put("indices.mapping.max_request_size", "10b").build());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(request(100), verifyingClient)
        );
        assertThat(e.getMessage(), containsString("indices.mapping.max_request_size"));
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { RestPutMappingAction.MAX_MAPPING_REQUEST_SIZE_SETTING });
    }
}
