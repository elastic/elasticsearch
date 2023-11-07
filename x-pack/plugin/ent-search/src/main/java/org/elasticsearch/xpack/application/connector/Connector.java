/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A connector document consists of:
 * <ul>
 *     <li>...</li>
 * </ul>
 */
public class Connector implements Writeable, ToXContentObject {

    private final String id;

    // private final String apiKeyId;
    //
    // private final ConnectorConfiguration configuration;
    //
    // private final ConnectorCustomScheduling customScheduling;
    //
    // private final String description;
    //
    // private final String error;
    //
    // private final ConnectorFeatures features;
    //
    // private final List<ConnectorFilteringConfig> filtering;
    // private final String indexName;
    //
    // private final boolean isNative;
    //
    // private final String language;
    //
    // private final String lastAccessControlSyncError;
    //
    // private final String lastAccessControlSyncScheduledAt;
    //
    // private final String lastAccessControlStatus;
    //
    // private final String lastIncrementalSyncScheduledAt;
    //
    // private final String lastSeen;
    //
    // private final String lastSyncError;
    //
    // private final String lastSyncError;
    //
    // private final String lastSyncScheduledAt;
    //
    // private final String lastSyncStatus;
    //
    // private final String lastSynced;
    //
    // private final String name;
    //
    // private final ConnectorIngestPipelineParams pipeline;
    //
    // private final ConnectorSchedulingConfiguration scheduling;
    //
    // private final String serviceType;
    //
    // private final ConnectorStatus status;
    //
    // private final boolean syncNow;

    public enum ConnectorStatus {
        CREATED,
        NEEDS_CONFIGURATION,
        CONFIGURED,
        CONNECTED,
        ERROR;

        public static Connector.ConnectorStatus connectorStatus(String status) {
            for (Connector.ConnectorStatus connectorStatus : Connector.ConnectorStatus.values()) {
                if (connectorStatus.name().equalsIgnoreCase(status)) {
                    return connectorStatus;
                }
            }
            throw new IllegalArgumentException("Unknown ConnectorStatus: " + status);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public Connector(String id) {
        this.id = id;
    }

    private static final ConstructingObjectParser<Connector, String> PARSER = new ConstructingObjectParser<>(
        "connector",
        false,
        (params, resourceName) -> {
            final String id = (String) params[0];
            // Check that id matches the resource name. We don't want it to be updatable
            if (id != null && id.equals(resourceName) == false) {
                throw new IllegalArgumentException("");
            }
            return new Connector(resourceName);
        }
    );

    public static final ParseField ID_FIELD = new ParseField("connector_id");

    static {
        PARSER.declareString(optionalConstructorArg(), ID_FIELD);
    }

    public static Connector fromXContentBytes(String resourceName, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return Connector.fromXContent(resourceName, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    public static Connector fromXContent(String resourceName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, resourceName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(ID_FIELD.getPreferredName(), id);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
    }

    public String id() {
        return id;
    }
}
