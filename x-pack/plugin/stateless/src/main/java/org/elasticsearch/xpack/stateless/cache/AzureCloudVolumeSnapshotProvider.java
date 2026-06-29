/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * {@link CloudVolumeSnapshotProvider} implementation for Azure Managed Disks.
 *
 * <p>Discovery proceeds in three steps:
 * <ol>
 *   <li>Azure IMDS — resolves the subscription ID, source resource group, and location for the
 *       current VM.</li>
 *   <li>Azure MSI — obtains a bearer token scoped to the Azure Resource Manager endpoint.</li>
 *   <li>Azure Compute REST API — creates an incremental snapshot of the configured managed disk
 *       and returns the provider-assigned resource ID.</li>
 * </ol>
 *
 * <p>All HTTP calls are made with the JDK built-in {@link java.net.http.HttpClient}; no
 * third-party HTTP library is required.
 */
public class AzureCloudVolumeSnapshotProvider implements CloudVolumeSnapshotProvider {

    public static final Setting<String> AZURE_DISK_NAME_SETTING = Setting.simpleString(
        "stateless.cache_snapshot.azure.disk_name",
        Setting.Property.NodeScope
    );

    public static final Setting<String> AZURE_SNAPSHOT_RESOURCE_GROUP_SETTING = Setting.simpleString(
        "stateless.cache_snapshot.azure.snapshot_resource_group",
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(AzureCloudVolumeSnapshotProvider.class);

    private static final String IMDS_INSTANCE_URL = "http://169.254.169.254/metadata/instance?api-version=2021-02-01";
    private static final String MSI_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
        + "?api-version=2018-02-01&resource=https%3A%2F%2Fmanagement.azure.com%2F";
    private static final String COMPUTE_API_VERSION = "2023-07-03";

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private final String diskName;
    private final String snapshotResourceGroup;
    private final LongSupplier epochSecondSupplier;

    /**
     * Constructs a provider from node settings.
     *
     * @param settings             node settings; {@link #AZURE_DISK_NAME_SETTING} is required
     * @param epochSecondSupplier  supplier for the current epoch second, used to generate
     *                             unique snapshot names — pass {@code Instant.now()::getEpochSecond}
     *                             in production
     * @throws IllegalArgumentException if {@link #AZURE_DISK_NAME_SETTING} is blank
     */
    public AzureCloudVolumeSnapshotProvider(Settings settings, LongSupplier epochSecondSupplier) {
        this.diskName = AZURE_DISK_NAME_SETTING.get(settings);
        if (diskName.isBlank()) {
            throw new IllegalArgumentException(
                "setting [" + AZURE_DISK_NAME_SETTING.getKey() + "] is required for AzureCloudVolumeSnapshotProvider"
            );
        }
        this.snapshotResourceGroup = AZURE_SNAPSHOT_RESOURCE_GROUP_SETTING.get(settings);
        this.epochSecondSupplier = epochSecondSupplier;
    }

    @Override
    public String createSnapshot() throws IOException {
        ImdsMetadata imds = fetchImdsMetadata();
        logger.debug(
            "IMDS: subscriptionId=[{}] resourceGroup=[{}] location=[{}]",
            imds.subscriptionId,
            imds.resourceGroupName,
            imds.location
        );

        String token = fetchMsiToken();
        logger.debug("obtained MSI token (length={})", token.length());

        String snapshotRg = snapshotResourceGroup.isBlank() ? imds.resourceGroupName : snapshotResourceGroup;
        String snapshotName = "es-cache-snapshot-" + epochSecondSupplier.getAsLong();

        String snapshotId = putSnapshot(token, imds.subscriptionId, imds.resourceGroupName, snapshotRg, imds.location, snapshotName);
        logger.debug("snapshot creation request submitted for disk [{}], snapshot ID [{}]", diskName, snapshotId);
        return snapshotId;
    }

    // -------------------------------------------------------------------------
    // IMDS
    // -------------------------------------------------------------------------

    private record ImdsMetadata(String subscriptionId, String resourceGroupName, String location) {}

    private ImdsMetadata fetchImdsMetadata() throws IOException {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(IMDS_INSTANCE_URL)).header("Metadata", "true").GET().build();

        HttpResponse<InputStream> response = send(request);
        if (response.statusCode() != 200) {
            throw new IOException("IMDS instance endpoint returned HTTP " + response.statusCode());
        }

        try (
            InputStream body = response.body();
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, body)
        ) {
            return parseImdsMetadata(parser);
        }
    }

    private static ImdsMetadata parseImdsMetadata(XContentParser parser) throws IOException {
        // Expected structure: { "compute": { "subscriptionId": "...", "resourceGroupName": "...", "location": "..." }, ... }
        String subscriptionId = null;
        String resourceGroupName = null;
        String location = null;

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IOException("unexpected IMDS response format: expected top-level object");
        }
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String topField = parser.currentName();
            parser.nextToken();
            if ("compute".equals(topField) && parser.currentToken() == XContentParser.Token.START_OBJECT) {
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    switch (fieldName) {
                        case "subscriptionId" -> subscriptionId = parser.text();
                        case "resourceGroupName" -> resourceGroupName = parser.text();
                        case "location" -> location = parser.text();
                        default -> parser.skipChildren();
                    }
                }
            } else {
                parser.skipChildren();
            }
        }

        if (subscriptionId == null || resourceGroupName == null || location == null) {
            throw new IOException("IMDS response is missing required fields (subscriptionId, resourceGroupName, location)");
        }
        return new ImdsMetadata(subscriptionId, resourceGroupName, location);
    }

    // -------------------------------------------------------------------------
    // MSI token
    // -------------------------------------------------------------------------

    private String fetchMsiToken() throws IOException {
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(MSI_TOKEN_URL)).header("Metadata", "true").GET().build();

        HttpResponse<InputStream> response = send(request);
        if (response.statusCode() != 200) {
            throw new IOException("MSI token endpoint returned HTTP " + response.statusCode());
        }

        try (
            InputStream body = response.body();
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, body)
        ) {
            return parseMsiToken(parser);
        }
    }

    private static String parseMsiToken(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IOException("unexpected MSI token response format: expected top-level object");
        }
        String accessToken = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("access_token".equals(fieldName)) {
                accessToken = parser.text();
            } else {
                parser.skipChildren();
            }
        }
        if (accessToken == null) {
            throw new IOException("MSI token response did not contain access_token");
        }
        return accessToken;
    }

    // -------------------------------------------------------------------------
    // Snapshot creation
    // -------------------------------------------------------------------------

    private String putSnapshot(
        String token,
        String subscriptionId,
        String sourceRg,
        String snapshotRg,
        String location,
        String snapshotName
    ) throws IOException {
        String url = "https://management.azure.com/subscriptions/"
            + subscriptionId
            + "/resourceGroups/"
            + snapshotRg
            + "/providers/Microsoft.Compute/snapshots/"
            + snapshotName
            + "?api-version="
            + COMPUTE_API_VERSION;

        String requestBody = buildSnapshotRequestBody(location, subscriptionId, sourceRg);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

        HttpResponse<InputStream> response = send(request);
        int status = response.statusCode();

        try (
            InputStream body = response.body();
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, body)
        ) {
            if (status < 200 || status >= 300) {
                String bodyText = readBodyAsString(parser);
                throw new IOException("snapshot API returned HTTP " + status + ": " + bodyText);
            }
            return parseSnapshotId(parser);
        }
    }

    private String buildSnapshotRequestBody(String location, String subscriptionId, String sourceRg) throws IOException {
        String sourceResourceId = "/subscriptions/"
            + subscriptionId
            + "/resourceGroups/"
            + sourceRg
            + "/providers/Microsoft.Compute/disks/"
            + diskName;

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("location", location);
            builder.startObject("properties");
            builder.startObject("creationData");
            builder.field("createOption", "Copy");
            builder.field("sourceResourceId", sourceResourceId);
            builder.endObject(); // creationData
            builder.endObject(); // properties
            builder.endObject();
            return builder.toString();
        }
    }

    private static String parseSnapshotId(XContentParser parser) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new IOException("unexpected snapshot API response format: expected top-level object");
        }
        String id = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("id".equals(fieldName)) {
                id = parser.text();
            } else {
                parser.skipChildren();
            }
        }
        if (id == null) {
            throw new IOException("snapshot API response did not contain id field");
        }
        return id;
    }

    /**
     * Reads all remaining tokens from the parser and assembles them into a plain string for
     * error messages. This is used only on error paths where the response body is small.
     */
    private static String readBodyAsString(XContentParser parser) {
        StringBuilder sb = new StringBuilder();
        try {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null) {
                switch (token) {
                    case FIELD_NAME -> sb.append(parser.currentName()).append(": ");
                    case VALUE_STRING -> sb.append(parser.text()).append(" ");
                    case VALUE_NUMBER -> sb.append(parser.numberValue()).append(" ");
                    case VALUE_BOOLEAN -> sb.append(parser.booleanValue()).append(" ");
                    default -> {
                        // structural tokens (start/end object/array) — skip
                    }
                }
            }
        } catch (IOException e) {
            sb.append("[could not read body: ").append(e.getMessage()).append("]");
        }
        return sb.toString().trim();
    }

    // -------------------------------------------------------------------------
    // HTTP helper
    // -------------------------------------------------------------------------

    private static HttpResponse<InputStream> send(HttpRequest request) throws IOException {
        try {
            return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted: " + request.uri(), e);
        }
    }
}
