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

package org.elasticsearch.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterClientIT extends ESRestHighLevelClientTestCase {

    public void testClusterUpdateSettings() throws IOException {
        final String transientSettingKey = RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey();
        final int transientSettingValue = 10;

        final String persistentSettingKey = EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey();
        final String persistentSettingValue = EnableAllocationDecider.Allocation.NONE.name();

        Settings transientSettings = Settings.builder().put(transientSettingKey, transientSettingValue, ByteSizeUnit.BYTES).build();
        Map<String, Object> map = new HashMap<>();
        map.put(persistentSettingKey, persistentSettingValue);

        ClusterUpdateSettingsRequest setRequest = new ClusterUpdateSettingsRequest();
        setRequest.transientSettings(transientSettings);
        setRequest.persistentSettings(map);

        ClusterUpdateSettingsResponse setResponse = execute(setRequest, highLevelClient().cluster()::updateSettings,
                highLevelClient().cluster()::updateSettingsAsync);

        assertAcked(setResponse);
        assertThat(setResponse.getTransientSettings().get(transientSettingKey), notNullValue());
        assertThat(setResponse.getTransientSettings().get(persistentSettingKey), nullValue());
        assertThat(setResponse.getTransientSettings().get(transientSettingKey),
                equalTo(transientSettingValue + ByteSizeUnit.BYTES.getSuffix()));
        assertThat(setResponse.getPersistentSettings().get(transientSettingKey), nullValue());
        assertThat(setResponse.getPersistentSettings().get(persistentSettingKey), notNullValue());
        assertThat(setResponse.getPersistentSettings().get(persistentSettingKey), equalTo(persistentSettingValue));

        Map<String, Object> setMap = getAsMap("/_cluster/settings");
        String transietSetValue = (String) XContentMapValues.extractValue("transient." + transientSettingKey, setMap);
        assertThat(transietSetValue, equalTo(transientSettingValue + ByteSizeUnit.BYTES.getSuffix()));
        String persistentSetValue = (String) XContentMapValues.extractValue("persistent." + persistentSettingKey, setMap);
        assertThat(persistentSetValue, equalTo(persistentSettingValue));

        ClusterUpdateSettingsRequest reserRequest = new ClusterUpdateSettingsRequest();
        reserRequest.transientSettings(Settings.builder().putNull(transientSettingKey));
        reserRequest.persistentSettings("{\"" + persistentSettingKey + "\": null }", XContentType.JSON);

        ClusterUpdateSettingsResponse resetResponse = execute(reserRequest, highLevelClient().cluster()::updateSettings,
                highLevelClient().cluster()::updateSettingsAsync);

        assertThat(resetResponse.getTransientSettings().get(transientSettingKey), equalTo(null));
        assertThat(resetResponse.getPersistentSettings().get(persistentSettingKey), equalTo(null));
        assertThat(resetResponse.getTransientSettings(), equalTo(Settings.EMPTY));
        assertThat(resetResponse.getPersistentSettings(), equalTo(Settings.EMPTY));

        Map<String, Object> resetMap = getAsMap("/_cluster/settings");
        String transietResetValue = (String) XContentMapValues.extractValue("transient." + transientSettingKey, resetMap);
        assertThat(transietResetValue, equalTo(null));
        String persistentResetValue = (String) XContentMapValues.extractValue("persistent." + persistentSettingKey, resetMap);
        assertThat(persistentResetValue, equalTo(null));
    }

    public void testClusterUpdateSettingNonExistent() {
        String setting = "no_idea_what_you_are_talking_about";
        int value = 10;
        ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.transientSettings(Settings.builder().put(setting, value).build());

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(clusterUpdateSettingsRequest,
                highLevelClient().cluster()::updateSettings, highLevelClient().cluster()::updateSettingsAsync));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), equalTo(
                "Elasticsearch exception [type=illegal_argument_exception, reason=transient setting [" + setting + "], not recognized]"));
    }
}
