/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.InputStream;
import java.util.Locale;

import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.SAS_TOKEN_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@SuppressForbidden(reason = "use a http server")
public class AzureSasTokenTests extends AbstractAzureServerTestCase {
    public void testSasTokenIsUsedAsProvidedInSettings() throws Exception {
        final int maxRetries = randomIntBetween(1, 5);
        final byte[] bytes = randomBlobContent();

        final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "account");
        final String sasTokenPrefix = randomBoolean() ? "?" : "";
        final String sasToken = sasTokenPrefix
            + "sv=2018-11-09&spr=https&st=2021-09-21T13%3A00%3A07Z&se=2071-09-21T13%3A00%3A07Z"
            + "&sr=c&sp=racwdl&sig=4%2Fak3ibKW%2FXILJI%2B8mInVaLiDw8n3Es%2FQbTSiG3LOt0%3D";
        secureSettings.setString(SAS_TOKEN_SETTING.getConcreteSettingForNamespace(clientName).getKey(), sasToken);

        httpServer.createContext("/account/container/sas_test", exchange -> {
            try {
                final String queryParams = exchange.getRequestURI().getRawQuery();
                if (sasToken.startsWith("?")) {
                    assertThat(queryParams, is(equalTo(sasToken.substring(1))));
                } else {
                    assertThat(queryParams, is(equalTo(sasToken)));
                }

                Streams.readFully(exchange.getRequestBody());
                if ("HEAD".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(bytes.length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    final int rangeStart = getRangeStart(exchange);
                    assertThat(rangeStart, lessThan(bytes.length));
                    final int length = bytes.length - rangeStart;
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.getResponseHeaders().add("x-ms-blob-content-length", String.valueOf(length));
                    exchange.getResponseHeaders().add("Content-Length", String.valueOf(length));
                    exchange.getResponseHeaders().add("x-ms-blob-type", "blockblob");
                    exchange.getResponseHeaders().add("ETag", UUIDs.base64UUID());
                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), length);
                    exchange.getResponseBody().write(bytes, rangeStart, length);
                }
            } finally {
                exchange.close();
            }
        });

        final BlobContainer blobContainer = createBlobContainer(maxRetries, null, LocationMode.PRIMARY_ONLY, clientName, secureSettings);
        try (InputStream inputStream = blobContainer.readBlob("sas_test")) {
            assertArrayEquals(bytes, BytesReference.toBytes(Streams.readFully(inputStream)));
        }
    }
}
