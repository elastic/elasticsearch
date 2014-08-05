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

package org.elasticsearch.cloud.gce;

import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class GceComputeServiceImpl extends AbstractLifecycleComponent<GceComputeServiceImpl>
    implements GceComputeService {

    private final String project;
    private final String zone;

    // Forcing Google Token API URL as set in GCE SDK to
    //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
    // See https://developers.google.com/compute/docs/metadata#metadataserver
    private static final String TOKEN_SERVER_ENCODED_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    @Override
    public Collection<Instance> instances() {
        try {
            logger.debug("get instances for project [{}], zone [{}]", project, zone);

            Compute.Instances.List list = client().instances().list(project, zone);
            InstanceList instanceList = list.execute();

            return instanceList.getItems();
        } catch (IOException e) {
            logger.warn("disabling GCE discovery. Can not get list of nodes: {}", e.getMessage());
            logger.debug("Full exception:", e);
            return new ArrayList<Instance>();
        }
    }

    private Compute client;
    private TimeValue refreshInterval = null;
    private long lastRefresh;

    /** Global instance of the HTTP transport. */
    private static HttpTransport HTTP_TRANSPORT;

    /** Global instance of the JSON factory. */
    private static JsonFactory JSON_FACTORY;

    @Inject
    public GceComputeServiceImpl(Settings settings, SettingsFilter settingsFilter) {
        super(settings);
        settingsFilter.addFilter(new GceSettingsFilter());

        this.project = componentSettings.get(Fields.PROJECT, settings.get("cloud.gce." + Fields.PROJECT));
        this.zone = componentSettings.get(Fields.ZONE, settings.get("cloud.gce." + Fields.ZONE));
    }

    public synchronized Compute client() {
        if (refreshInterval != null && refreshInterval.millis() != 0) {
            if (client != null &&
                    (refreshInterval.millis() < 0 || (System.currentTimeMillis() - lastRefresh) < refreshInterval.millis())) {
                if (logger.isTraceEnabled()) logger.trace("using cache to retrieve client");
                return client;
            }
            lastRefresh = System.currentTimeMillis();
        }

        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            JSON_FACTORY = new JacksonFactory();

            logger.info("starting GCE discovery service");

            ComputeCredential credential = new ComputeCredential.Builder(HTTP_TRANSPORT, JSON_FACTORY)
                    .setTokenServerEncodedUrl(TOKEN_SERVER_ENCODED_URL)
                    .build();

            credential.refreshToken();

            logger.debug("token [{}] will expire in [{}] s", credential.getAccessToken(), credential.getExpiresInSeconds());
            refreshInterval = TimeValue.timeValueSeconds(credential.getExpiresInSeconds()-1);

            // Once done, let's use this token
            this.client = new Compute.Builder(HTTP_TRANSPORT, JSON_FACTORY, null)
                    .setApplicationName(Fields.VERSION)
                    .setHttpRequestInitializer(credential)
                    .build();
        } catch (Exception e) {
            logger.warn("unable to start GCE discovery service: {} : {}", e.getClass().getName(), e.getMessage());
            throw new DiscoveryException("unable to start GCE discovery service", e);
        }

        return this.client;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
