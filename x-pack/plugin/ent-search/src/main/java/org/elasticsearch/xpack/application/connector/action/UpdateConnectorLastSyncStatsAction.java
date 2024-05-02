/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.application.connector.ConnectorSyncInfo;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;
import org.elasticsearch.xpack.application.connector.ConnectorUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateConnectorLastSyncStatsAction {

    public static final String NAME = "indices:data/write/xpack/connector/update_last_sync_stats";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorLastSyncStatsAction() {/* no instances */}

    public static class Request extends ConnectorActionRequest implements ToXContentObject {

        private final String connectorId;

        private final ConnectorSyncInfo syncInfo;

        public Request(String connectorId, ConnectorSyncInfo syncInfo) {
            this.connectorId = connectorId;
            this.syncInfo = syncInfo;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorId = in.readString();
            this.syncInfo = in.readOptionalWriteable(ConnectorSyncInfo::new);
        }

        public String getConnectorId() {
            return connectorId;
        }

        public ConnectorSyncInfo getSyncInfo() {
            return syncInfo;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorId)) {
                validationException = addValidationError("[connector_id] cannot be [null] or [\"\"].", validationException);
            }

            return validationException;
        }

        private static final ConstructingObjectParser<UpdateConnectorLastSyncStatsAction.Request, String> PARSER =
            new ConstructingObjectParser<>("connector_update_last_sync_stats_request", false, ((args, connectorId) -> {
                int i = 0;
                return new UpdateConnectorLastSyncStatsAction.Request(
                    connectorId,
                    new ConnectorSyncInfo.Builder().setLastAccessControlSyncError((String) args[i++])
                        .setLastAccessControlSyncScheduledAt((Instant) args[i++])
                        .setLastAccessControlSyncStatus((ConnectorSyncStatus) args[i++])
                        .setLastDeletedDocumentCount((Long) args[i++])
                        .setLastIncrementalSyncScheduledAt((Instant) args[i++])
                        .setLastIndexedDocumentCount((Long) args[i++])
                        .setLastSyncError((String) args[i++])
                        .setLastSyncScheduledAt((Instant) args[i++])
                        .setLastSyncStatus((ConnectorSyncStatus) args[i++])
                        .setLastSynced((Instant) args[i++])
                        .build()
                );
            }));

        static {
            PARSER.declareStringOrNull(optionalConstructorArg(), ConnectorSyncInfo.LAST_ACCESS_CONTROL_SYNC_ERROR);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorUtils.parseNullableInstant(
                    p,
                    ConnectorSyncInfo.LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD.getPreferredName()
                ),
                ConnectorSyncInfo.LAST_ACCESS_CONTROL_SYNC_SCHEDULED_AT_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : ConnectorSyncStatus.connectorSyncStatus(p.text()),
                ConnectorSyncInfo.LAST_ACCESS_CONTROL_SYNC_STATUS_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareLong(optionalConstructorArg(), ConnectorSyncInfo.LAST_DELETED_DOCUMENT_COUNT_FIELD);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorUtils.parseNullableInstant(
                    p,
                    ConnectorSyncInfo.LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD.getPreferredName()
                ),
                ConnectorSyncInfo.LAST_INCREMENTAL_SYNC_SCHEDULED_AT_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareLong(optionalConstructorArg(), ConnectorSyncInfo.LAST_INDEXED_DOCUMENT_COUNT_FIELD);
            PARSER.declareStringOrNull(optionalConstructorArg(), ConnectorSyncInfo.LAST_SYNC_ERROR_FIELD);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorUtils.parseNullableInstant(p, ConnectorSyncInfo.LAST_SYNC_SCHEDULED_AT_FIELD.getPreferredName()),
                ConnectorSyncInfo.LAST_SYNC_SCHEDULED_AT_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? null : ConnectorSyncStatus.connectorSyncStatus(p.text()),
                ConnectorSyncInfo.LAST_SYNC_STATUS_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorUtils.parseNullableInstant(p, ConnectorSyncInfo.LAST_SYNCED_FIELD.getPreferredName()),
                ConnectorSyncInfo.LAST_SYNCED_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
        }

        public static UpdateConnectorLastSyncStatsAction.Request fromXContentBytes(
            String connectorId,
            BytesReference source,
            XContentType xContentType
        ) {
            try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
                return UpdateConnectorLastSyncStatsAction.Request.fromXContent(parser, connectorId);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
            }
        }

        public static UpdateConnectorLastSyncStatsAction.Request fromXContent(XContentParser parser, String connectorId)
            throws IOException {
            return PARSER.parse(parser, connectorId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                syncInfo.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorId);
            out.writeOptionalWriteable(syncInfo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorId, request.connectorId) && Objects.equals(syncInfo, request.syncInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectorId, syncInfo);
        }
    }
}
