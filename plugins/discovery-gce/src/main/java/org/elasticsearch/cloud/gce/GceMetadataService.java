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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.GeneralSecurityException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.function.Function;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cloud.gce.util.Access;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

public class GceMetadataService extends AbstractLifecycleComponent {

    // Forcing Google Token API URL as set in GCE SDK to
    //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
    // See https://developers.google.com/compute/docs/metadata#metadataserver
    // all settings just used for testing - not registered by default
    public static final Setting<String> GCE_HOST =
        new Setting<>("cloud.gce.host", "http://metadata.google.internal", Function.identity(), Setting.Property.NodeScope);

    /** Global instance of the HTTP transport. */
    private HttpTransport gceHttpTransport;

    public GceMetadataService(Settings settings) {
        super(settings);
    }

    protected synchronized HttpTransport getGceHttpTransport() throws GeneralSecurityException, IOException {
        if (gceHttpTransport == null) {
            gceHttpTransport = GoogleNetHttpTransport.newTrustedTransport();
        }
        return gceHttpTransport;
    }

    public String metadata(String metadataPath) throws IOException, URISyntaxException {
        // Forcing Google Token API URL as set in GCE SDK to
        //      http://metadata/computeMetadata/v1/instance/service-accounts/default/token
        // See https://developers.google.com/compute/docs/metadata#metadataserver
        final URI urlMetadataNetwork = new URI(GCE_HOST.get(settings)).resolve("/computeMetadata/v1/instance/").resolve(metadataPath);
        logger.debug("get metadata from [{}]", urlMetadataNetwork);
        HttpHeaders headers;
        try {
            // hack around code messiness in GCE code
            // TODO: get this fixed
            headers = Access.doPrivileged(HttpHeaders::new);
            GenericUrl genericUrl = Access.doPrivileged(() -> new GenericUrl(urlMetadataNetwork));

            // This is needed to query meta data: https://cloud.google.com/compute/docs/metadata
            headers.put("Metadata-Flavor", "Google");
            HttpResponse response = Access.doPrivilegedIOException(() ->
                getGceHttpTransport().createRequestFactory()
                    .buildGetRequest(genericUrl)
                    .setHeaders(headers)
                    .execute());
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
