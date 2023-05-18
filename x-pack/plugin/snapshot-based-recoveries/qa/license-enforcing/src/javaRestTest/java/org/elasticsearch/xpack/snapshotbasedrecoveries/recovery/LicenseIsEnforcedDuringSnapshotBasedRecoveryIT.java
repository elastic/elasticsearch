/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries.recovery;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class LicenseIsEnforcedDuringSnapshotBasedRecoveryIT extends AbstractSnapshotBasedRecoveryRestTestCase {

    @Override
    protected String repositoryType() {
        return "fs";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder().put("location", System.getProperty("tests.path.repo")).build();
    }

    @Override
    protected void checkSnapshotUsageDuringRecovery(String index) throws Exception {
        Request request = new Request(HttpGet.METHOD_NAME, '/' + index + "/_recovery?detailed=true");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> responseAsMap = responseAsMap(response);
        List<Map<String, Object>> shardRecoveries = extractValue(responseAsMap, index + ".shards");
        long totalRecoveredFromSnapshot = 0;
        long totalRecovered = 0;
        for (Map<String, Object> shardRecoveryState : shardRecoveries) {
            String recoveryType = extractValue(shardRecoveryState, "type");
            if (recoveryType.equals("PEER") == false) {
                continue;
            }
            String stage = extractValue(shardRecoveryState, "stage");
            assertThat(stage, is(equalTo("DONE")));

            List<Map<String, Object>> fileDetails = extractValue(shardRecoveryState, "index.files.details");
            for (Map<String, Object> fileDetail : fileDetails) {
                int recoveredFromSource = extractValue(fileDetail, "recovered_in_bytes");
                int recoveredFromSnapshot = extractValue(fileDetail, "recovered_from_snapshot_in_bytes");
                assertThat(recoveredFromSnapshot, is(equalTo(0)));
                totalRecoveredFromSnapshot += recoveredFromSnapshot;
                totalRecovered += recoveredFromSource;
            }
        }
        assertThat(totalRecoveredFromSnapshot, is(equalTo(0L)));
        assertThat(totalRecovered, is(greaterThan(0L)));
    }
}
