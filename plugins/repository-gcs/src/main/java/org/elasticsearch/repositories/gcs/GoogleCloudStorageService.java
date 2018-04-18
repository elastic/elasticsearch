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

package org.elasticsearch.repositories.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.threeten.bp.Duration;

import java.util.Map;

public class GoogleCloudStorageService extends AbstractComponent {

    /** Clients settings identified by client name. */
    private final Map<String, GoogleCloudStorageClientSettings> clientsSettings;
    private final RetrySettings retrySettings;

    public GoogleCloudStorageService(Environment environment, Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        super(environment.settings());
        this.clientsSettings = clientsSettings;
        this.retrySettings = RetrySettings.newBuilder()
                .setInitialRetryDelay(Duration.ofMillis(100))
                .setMaxRetryDelay(Duration.ofMillis(6000))
                .setTotalTimeout(Duration.ofMillis(900000))
                .setRetryDelayMultiplier(1.5d)
                .setJittered(true)
                .build();
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects.
     *
     * @param clientName
     *            name of client settings to use from secure settings
     * @return a Client instance that can be used to manage Storage objects
     */
    public Storage createClient(String clientName) {
        final GoogleCloudStorageClientSettings clientSettings = clientsSettings.get(clientName);
        if (clientSettings == null) {
            throw new IllegalArgumentException("Unknown client name [" + clientName + "]. Existing client configs: "
                    + Strings.collectionToDelimitedString(clientsSettings.keySet(), ","));
        }
        final HttpTransportOptions httpTransportOptions = HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(clientSettings.getConnectTimeout()))
                .setReadTimeout(toTimeout(clientSettings.getReadTimeout()))
                .build();
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
                .setRetrySettings(retrySettings)
                .setTransportOptions(httpTransportOptions)
                .setHeaderProvider(() -> {
                    final MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
                    if (Strings.hasLength(clientSettings.getApplicationName())) {
                        mapBuilder.put("user-agent", clientSettings.getApplicationName());
                    }
                    return mapBuilder.immutableMap();
                });
        if (Strings.hasLength(clientSettings.getProjectId())) {
            storageOptionsBuilder.setProjectId(clientSettings.getProjectId());
        }
        if (Strings.hasLength(clientSettings.getHost())) {
            storageOptionsBuilder.setHost(clientSettings.getHost());
        }
        if (clientSettings.getCredential() != null) {
            storageOptionsBuilder.setCredentials(clientSettings.getCredential());
        }
        return storageOptionsBuilder.build().getService();
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    static Integer toTimeout(TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if ((timeout == null) || TimeValue.ZERO.equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.getMillis());
    }

}
