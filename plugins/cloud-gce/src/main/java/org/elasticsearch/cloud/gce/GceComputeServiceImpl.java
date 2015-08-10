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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class GceComputeServiceImpl extends AbstractLifecycleComponent<GceComputeService>
    implements GceComputeService {

    private final String project;
    private final List<String> zones;

    // Forcing Google Token API URL as set in GCE SDK to
    //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
    // See https://developers.google.com/compute/docs/metadata#metadataserver
    public static final String TOKEN_SERVER_ENCODED_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

    @Override
    public Collection<Instance> instances() {

            logger.debug("get instances for project [{}], zones [{}]", project, zones);

            List<List<Instance>> instanceListByZone = Lists.transform(zones, new Function<String, List<Instance>>() {
                @Override
                public List<Instance> apply(String zoneId) {
                    try {
                        Compute.Instances.List list = client().instances().list(project, zoneId);
                        InstanceList instanceList = list.execute();
                        if (instanceList.isEmpty()) {
                            return Collections.EMPTY_LIST;
                        }

                        return instanceList.getItems();
                    } catch (IOException e) {
                        logger.warn("Problem fetching instance list for zone {}", zoneId);
                        logger.debug("Full exception:", e);

                        return Collections.EMPTY_LIST;
                    }
                }
            });

            // Collapse instances from all zones into one neat list
            List<Instance> instanceList = Lists.newArrayList(Iterables.concat(instanceListByZone));

            if (instanceList.size() == 0) {
                logger.warn("disabling GCE discovery. Can not get list of nodes");
            }

            return instanceList;
    }

    private Compute client;
    private TimeValue refreshInterval = null;
    private long lastRefresh;

    /** Global instance of the HTTP transport. */
    private HttpTransport gceHttpTransport;

    /** Global instance of the JSON factory. */
    private JsonFactory gceJsonFactory;

    @Inject
    public GceComputeServiceImpl(Settings settings) {
        super(settings);
        this.project = settings.get(Fields.PROJECT);
        String[] zoneList = settings.getAsArray(Fields.ZONE);
        this.zones = Arrays.asList(zoneList);
    }

    protected synchronized HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (gceHttpTransport == null) {
            gceHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
        }
        return gceHttpTransport;
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
            gceJsonFactory = new JacksonFactory();

            logger.info("starting GCE discovery service");
            ComputeCredential credential = new ComputeCredential.Builder(getGceHttpTransport(), gceJsonFactory)
                        .setTokenServerEncodedUrl(TOKEN_SERVER_ENCODED_URL)
                    .build();

            credential.refreshToken();

            logger.debug("token [{}] will expire in [{}] s", credential.getAccessToken(), credential.getExpiresInSeconds());
            if (credential.getExpiresInSeconds() != null) {
                refreshInterval = TimeValue.timeValueSeconds(credential.getExpiresInSeconds()-1);
            }

            // Once done, let's use this token
            this.client = new Compute.Builder(getGceHttpTransport(), gceJsonFactory, null)
                    .setApplicationName(Fields.VERSION)
                    .setHttpRequestInitializer(credential)
                    .build();
        } catch (Exception e) {
            logger.warn("unable to start GCE discovery service", e);
            throw new IllegalArgumentException("unable to start GCE discovery service", e);
        }

        return this.client;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (gceHttpTransport != null) {
            try {
                gceHttpTransport.shutdown();
            } catch (IOException e) {
                logger.warn("unable to shutdown GCE Http Transport", e);
            }
            gceHttpTransport = null;
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
