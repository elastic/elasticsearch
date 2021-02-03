/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.DeprecationInfoResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;

public class MigrationIT extends ESRestHighLevelClientTestCase {

    public void testGetDeprecationInfo() throws IOException {
        createIndex("test", Settings.EMPTY);
        DeprecationInfoRequest request = new DeprecationInfoRequest(Collections.singletonList("test"));
        DeprecationInfoResponse response = highLevelClient().migration().getDeprecationInfo(request, RequestOptions.DEFAULT);

        List<DeprecationInfoResponse.DeprecationIssue> nodeSettingsIssues = response.getNodeSettingsIssues();
        if (JavaVersion.current().compareTo(JavaVersion.parse("11")) < 0) {
            nodeSettingsIssues = nodeSettingsIssues.stream()
                .filter(each -> each.getMessage().equals("Java 11 is required") == false)
                .collect(Collectors.toList());
        }

        // a test like this cannot test actual deprecations
        assertThat(response.getClusterSettingsIssues(), empty());
        assertThat(response.getIndexSettingsIssues(), anEmptyMap());
        assertThat(nodeSettingsIssues, empty());
        assertThat(response.getMlSettingsIssues(), empty());
    }
}
