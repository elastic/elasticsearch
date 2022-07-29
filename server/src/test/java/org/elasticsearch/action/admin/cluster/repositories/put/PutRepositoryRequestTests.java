/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class PutRepositoryRequestTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testCreateRepositoryToXContent() throws IOException {
        Map<String, String> mapParams = new HashMap<>();
        PutRepositoryRequest request = new PutRepositoryRequest();
        String repoName = "test";
        request.name(repoName);
        mapParams.put("name", repoName);
        Boolean verify = randomBoolean();
        request.verify(verify);
        mapParams.put("verify", verify.toString());
        String type = FsRepository.TYPE;
        request.type(type);
        mapParams.put("type", type);

        Boolean addSettings = randomBoolean();
        if (addSettings) {
            request.settings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), ".").build());
        }

        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, new ToXContent.MapParams(mapParams));
        builder.flush();

        Map<String, Object> outputMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(outputMap.get("name"), equalTo(request.name()));
        assertThat(outputMap.get("verify"), equalTo(request.verify()));
        assertThat(outputMap.get("type"), equalTo(request.type()));
        Map<String, Object> settings = (Map<String, Object>) outputMap.get("settings");
        if (addSettings) {
            assertThat(settings.get(FsRepository.LOCATION_SETTING.getKey()), equalTo("."));
        } else {
            assertTrue(((Map<String, Object>) outputMap.get("settings")).isEmpty());
        }
    }
}
