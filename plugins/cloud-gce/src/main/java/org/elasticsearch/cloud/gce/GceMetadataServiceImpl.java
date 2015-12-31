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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import org.elasticsearch.cloud.gce.network.GceNameResolver;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.net.URL;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;

public class GceMetadataServiceImpl extends AbstractLifecycleComponent<GceMetadataService>
    implements GceMetadataService {

    // Forcing Google Token API URL as set in GCE SDK to
    //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
    // See https://developers.google.com/compute/docs/metadata#metadataserver
    public static final String GCE_METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance";
    public static final String TOKEN_SERVER_ENCODED_URL = GCE_METADATA_URL + "/service-accounts/default/token";

    /** Global instance of the HTTP transport. */
    private HttpTransport gceHttpTransport;

    @Inject
    public GceMetadataServiceImpl(Settings settings, NetworkService networkService) {
        super(settings);
        networkService.addCustomNameResolver(new GceNameResolver(settings, this));
    }

    protected synchronized HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (gceHttpTransport == null) {
            gceHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
        }
        return gceHttpTransport;
    }

    @Override
    public String metadata(String metadataPath) throws IOException {
        String urlMetadataNetwork = GCE_METADATA_URL + "/" + metadataPath;
        logger.debug("get metadata from [{}]", urlMetadataNetwork);
        URL url = new URL(urlMetadataNetwork);
        HttpHeaders headers;
        try {
            // hack around code messiness in GCE code
            // TODO: get this fixed
            headers = AccessController.doPrivileged(new PrivilegedExceptionAction<HttpHeaders>() {
                @Override
                public HttpHeaders run() throws IOException {
                    return new HttpHeaders();
                }
            });

            // This is needed to query meta data: https://cloud.google.com/compute/docs/metadata
            headers.put("Metadata-Flavor", "Google");
            HttpResponse response;
            response = getGceHttpTransport().createRequestFactory()
                .buildGetRequest(new GenericUrl(url))
                .setHeaders(headers)
                .execute();
            String metadata = response.parseAsString();
            logger.debug("metadata found [{}]", metadata);
            return metadata;
        } catch (Exception e) {
            throw new IOException("failed to fetch metadata from [" + urlMetadataNetwork + "]", e);
        }
    }


    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {
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
    protected void doClose() {

    }
}
