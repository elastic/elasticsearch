/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.recovery.RecoverySettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * This class is used to generate the Java Cluster API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/ClusterClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class ClusterClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testClusterPutSettings() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::put-settings-request
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        // end::put-settings-request

        // tag::put-settings-create-settings
        String transientSettingKey =
                RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey();
        int transientSettingValue = 10;
        Settings transientSettings =
                Settings.builder()
                .put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES)
                .build(); // <1>

        String persistentSettingKey =
                EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey();
        String persistentSettingValue =
                EnableAllocationDecider.Allocation.NONE.name();
        Settings persistentSettings =
                Settings.builder()
                .put(persistentSettingKey, persistentSettingValue)
                .build(); // <2>
        // end::put-settings-create-settings

        // tag::put-settings-request-cluster-settings
        request.transientSettings(transientSettings); // <1>
        request.persistentSettings(persistentSettings); // <2>
        // end::put-settings-request-cluster-settings

        {
            // tag::put-settings-settings-builder
            Settings.Builder transientSettingsBuilder =
                    Settings.builder()
                    .put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES);
            request.transientSettings(transientSettingsBuilder); // <1>
            // end::put-settings-settings-builder
        }
        {
            // tag::put-settings-settings-map
            Map<String, Object> map = new HashMap<>();
            map.put(transientSettingKey
                    , transientSettingValue + ByteSizeUnit.BYTES.getSuffix());
            request.transientSettings(map); // <1>
            // end::put-settings-settings-map
        }
        {
            // tag::put-settings-settings-source
            request.transientSettings(
                    "{\"indices.recovery.max_bytes_per_sec\": \"10b\"}"
                    , XContentType.JSON); // <1>
            // end::put-settings-settings-source
        }

        // tag::put-settings-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::put-settings-request-timeout
        // tag::put-settings-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::put-settings-request-masterTimeout

        // tag::put-settings-execute
        ClusterUpdateSettingsResponse response = client.cluster().putSettings(request, RequestOptions.DEFAULT);
        // end::put-settings-execute

        // tag::put-settings-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        Settings transientSettingsResponse = response.getTransientSettings(); // <2>
        Settings persistentSettingsResponse = response.getPersistentSettings(); // <3>
        // end::put-settings-response
        assertTrue(acknowledged);
        assertThat(transientSettingsResponse.get(transientSettingKey), equalTo(transientSettingValue + ByteSizeUnit.BYTES.getSuffix()));
        assertThat(persistentSettingsResponse.get(persistentSettingKey), equalTo(persistentSettingValue));

        // tag::put-settings-request-reset-transient
        request.transientSettings(Settings.builder().putNull(transientSettingKey).build()); // <1>
        // tag::put-settings-request-reset-transient
        request.persistentSettings(Settings.builder().putNull(persistentSettingKey));
        ClusterUpdateSettingsResponse resetResponse = client.cluster().putSettings(request, RequestOptions.DEFAULT);

        assertTrue(resetResponse.isAcknowledged());
    }

    public void testClusterUpdateSettingsAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();

            // tag::put-settings-execute-listener
            ActionListener<ClusterUpdateSettingsResponse> listener =
                    new ActionListener<ClusterUpdateSettingsResponse>() {
                @Override
                public void onResponse(ClusterUpdateSettingsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-settings-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-settings-execute-async
            client.cluster().putSettingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-settings-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
