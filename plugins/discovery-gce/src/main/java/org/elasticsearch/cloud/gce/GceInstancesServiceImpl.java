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
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.gce.RetryHttpInitializerWrapper;

import java.io.IOException;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class GceInstancesServiceImpl extends AbstractLifecycleComponent implements GceInstancesService {

    // all settings just used for testing - not registered by default
    public static final Setting<Boolean> GCE_VALIDATE_CERTIFICATES =
        Setting.boolSetting("cloud.gce.validate_certificates", true, Property.NodeScope);
    public static final Setting<String> GCE_ROOT_URL =
        new Setting<>("cloud.gce.root_url", "https://www.googleapis.com", Function.identity(), Property.NodeScope);

    private final String project;
    private final List<String> zones;

    @Override
    public Collection<Instance> instances() {
        logger.debug("get instances for project [{}], zones [{}]", project, zones);
        final List<Instance> instances = zones.stream().map((zoneId) -> {
            try {
                // hack around code messiness in GCE code
                // TODO: get this fixed
                SecurityManager sm = System.getSecurityManager();
                if (sm != null) {
                    sm.checkPermission(new SpecialPermission());
                }
                InstanceList instanceList = AccessController.doPrivileged(new PrivilegedExceptionAction<InstanceList>() {
                    @Override
                    public InstanceList run() throws Exception {
                        Compute.Instances.List list = client().instances().list(project, zoneId);
                        return list.execute();
                    }
                });
                // assist type inference
                return instanceList.isEmpty()  || instanceList.getItems() == null ?
                    Collections.<Instance>emptyList() : instanceList.getItems();
            } catch (PrivilegedActionException e) {
                logger.warn("Problem fetching instance list for zone {}", e, zoneId);
                logger.debug("Full exception:", e);
                // assist type inference
                return Collections.<Instance>emptyList();
            }
        }).reduce(new ArrayList<>(), (a, b) -> {
            a.addAll(b);
            return a;
        });

        if (instances.isEmpty()) {
            logger.warn("disabling GCE discovery. Can not get list of nodes");
        }

        return instances;
    }

    private Compute client;
    private TimeValue refreshInterval = null;
    private long lastRefresh;

    /** Global instance of the HTTP transport. */
    private HttpTransport gceHttpTransport;

    /** Global instance of the JSON factory. */
    private JsonFactory gceJsonFactory;

    private final boolean validateCerts;

    @Inject
    public GceInstancesServiceImpl(Settings settings) {
        super(settings);
        this.project = PROJECT_SETTING.get(settings);
        this.zones = ZONE_SETTING.get(settings);
        this.validateCerts = GCE_VALIDATE_CERTIFICATES.get(settings);
    }

    protected synchronized HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (gceHttpTransport == null) {
            if (validateCerts) {
                gceHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
            } else {
                // this is only used for testing - alternative we could use the defaul keystore but this requires special configs too..
                gceHttpTransport = new NetHttpTransport.Builder().doNotValidateCertificate().build();
            }
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
            // Forcing Google Token API URL as set in GCE SDK to
            //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
            // See https://developers.google.com/compute/docs/metadata#metadataserver
            String tokenServerEncodedUrl = GceMetadataService.GCE_HOST.get(settings) +
                "/computeMetadata/v1/instance/service-accounts/default/token";
            ComputeCredential credential = new ComputeCredential.Builder(getGceHttpTransport(), gceJsonFactory)
                    .setTokenServerEncodedUrl(tokenServerEncodedUrl)
                    .build();

            // hack around code messiness in GCE code
            // TODO: get this fixed
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPermission(new SpecialPermission());
            }
            AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws IOException {
                    credential.refreshToken();
                    return null;
                }
            });

            logger.debug("token [{}] will expire in [{}] s", credential.getAccessToken(), credential.getExpiresInSeconds());
            if (credential.getExpiresInSeconds() != null) {
                refreshInterval = TimeValue.timeValueSeconds(credential.getExpiresInSeconds() - 1);
            }


            Compute.Builder builder = new Compute.Builder(getGceHttpTransport(), gceJsonFactory, null).setApplicationName(VERSION)
                .setRootUrl(GCE_ROOT_URL.get(settings));

            if (RETRY_SETTING.exists(settings)) {
                TimeValue maxWait = MAX_WAIT_SETTING.get(settings);
                RetryHttpInitializerWrapper retryHttpInitializerWrapper;

                if (maxWait.getMillis() > 0) {
                    retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential, maxWait);
                } else {
                    retryHttpInitializerWrapper = new RetryHttpInitializerWrapper(credential);
                }
                builder.setHttpRequestInitializer(retryHttpInitializerWrapper);

            } else {
                builder.setHttpRequestInitializer(credential);
            }

            this.client = builder.build();
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
