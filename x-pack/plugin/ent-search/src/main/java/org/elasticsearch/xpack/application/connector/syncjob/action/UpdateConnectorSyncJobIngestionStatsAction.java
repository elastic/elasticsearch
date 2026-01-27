/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.ConnectorUtils;
import org.elasticsearch.xpack.application.connector.action.ConnectorUpdateActionResponse;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobConstants.EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE;

public class UpdateConnectorSyncJobIngestionStatsAction {

    public static final String NAME = "cluster:admin/xpack/connector/sync_job/update_stats";
    public static final ActionType<ConnectorUpdateActionResponse> INSTANCE = new ActionType<>(NAME);

    private UpdateConnectorSyncJobIngestionStatsAction() {/* no instances */}

    public static class Request extends ConnectorSyncJobActionRequest implements ToXContentObject {
        public static final ParseField CONNECTOR_SYNC_JOB_ID_FIELD = new ParseField("connector_sync_job_id");
        public static final String DELETED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE = "[deleted_document_count] cannot be negative.";
        public static final String INDEXED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE = "[indexed_document_count] cannot be negative.";
        public static final String INDEXED_DOCUMENT_VOLUME_NEGATIVE_ERROR_MESSAGE = "[indexed_document_volume] cannot be negative.";
        public static final String TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE = "[total_document_count] cannot be negative.";

        private final String connectorSyncJobId;
        private final Long deletedDocumentCount;
        private final Long indexedDocumentCount;
        private final Long indexedDocumentVolume;
        private final Long totalDocumentCount;
        private final Instant lastSeen;
        private final Map<String, Object> metadata;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.connectorSyncJobId = in.readString();
            this.deletedDocumentCount = in.readLong();
            this.indexedDocumentCount = in.readLong();
            this.indexedDocumentVolume = in.readLong();
            this.totalDocumentCount = in.readOptionalLong();
            this.lastSeen = in.readOptionalInstant();
            this.metadata = in.readGenericMap();
        }

        public Request(
            String connectorSyncJobId,
            Long deletedDocumentCount,
            Long indexedDocumentCount,
            Long indexedDocumentVolume,
            Long totalDocumentCount,
            Instant lastSeen,
            Map<String, Object> metadata
        ) {
            this.connectorSyncJobId = connectorSyncJobId;
            this.deletedDocumentCount = deletedDocumentCount;
            this.indexedDocumentCount = indexedDocumentCount;
            this.indexedDocumentVolume = indexedDocumentVolume;
            this.totalDocumentCount = totalDocumentCount;
            this.lastSeen = lastSeen;
            this.metadata = metadata;
        }

        public String getConnectorSyncJobId() {
            return connectorSyncJobId;
        }

        public Long getDeletedDocumentCount() {
            return deletedDocumentCount;
        }

        public Long getIndexedDocumentCount() {
            return indexedDocumentCount;
        }

        public Long getIndexedDocumentVolume() {
            return indexedDocumentVolume;
        }

        public Long getTotalDocumentCount() {
            return totalDocumentCount;
        }

        public Instant getLastSeen() {
            return lastSeen;
        }

        public Map<String, Object> getMetadata() {
            return metadata;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;

            if (Strings.isNullOrEmpty(connectorSyncJobId)) {
                validationException = addValidationError(EMPTY_CONNECTOR_SYNC_JOB_ID_ERROR_MESSAGE, validationException);
            }

            if (deletedDocumentCount < 0L) {
                validationException = addValidationError(DELETED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE, validationException);
            }

            if (indexedDocumentCount < 0L) {
                validationException = addValidationError(INDEXED_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE, validationException);
            }

            if (indexedDocumentVolume < 0L) {
                validationException = addValidationError(INDEXED_DOCUMENT_VOLUME_NEGATIVE_ERROR_MESSAGE, validationException);
            }

            if (Objects.nonNull(totalDocumentCount) && totalDocumentCount < 0L) {
                validationException = addValidationError(TOTAL_DOCUMENT_COUNT_NEGATIVE_ERROR_MESSAGE, validationException);
            }

            return validationException;
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<UpdateConnectorSyncJobIngestionStatsAction.Request, String> PARSER =
            new ConstructingObjectParser<>("connector_sync_job_update_ingestion_stats", false, (args, connectorSyncJobId) -> {
                Long deletedDocumentCount = (Long) args[0];
                Long indexedDocumentCount = (Long) args[1];
                Long indexedDocumentVolume = (Long) args[2];

                Long totalDocumentVolume = args[3] != null ? (Long) args[3] : null;
                Instant lastSeen = args[4] != null ? (Instant) args[4] : null;
                Map<String, Object> metadata = (Map<String, Object>) args[5];

                return new Request(
                    connectorSyncJobId,
                    deletedDocumentCount,
                    indexedDocumentCount,
                    indexedDocumentVolume,
                    totalDocumentVolume,
                    lastSeen,
                    metadata
                );
            });

        static {
            PARSER.declareLong(constructorArg(), ConnectorSyncJob.DELETED_DOCUMENT_COUNT_FIELD);
            PARSER.declareLong(constructorArg(), ConnectorSyncJob.INDEXED_DOCUMENT_COUNT_FIELD);
            PARSER.declareLong(constructorArg(), ConnectorSyncJob.INDEXED_DOCUMENT_VOLUME_FIELD);
            PARSER.declareLong(optionalConstructorArg(), ConnectorSyncJob.TOTAL_DOCUMENT_COUNT_FIELD);
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> ConnectorUtils.parseInstant(p, Connector.LAST_SEEN_FIELD.getPreferredName()),
                ConnectorSyncJob.LAST_SEEN_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), ConnectorSyncJob.METADATA_FIELD);
        }

        public static Request fromXContent(XContentParser parser, String connectorSyncJobId) throws IOException {
            return PARSER.parse(parser, connectorSyncJobId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(ConnectorSyncJob.DELETED_DOCUMENT_COUNT_FIELD.getPreferredName(), deletedDocumentCount);
                builder.field(ConnectorSyncJob.INDEXED_DOCUMENT_COUNT_FIELD.getPreferredName(), indexedDocumentCount);
                builder.field(ConnectorSyncJob.INDEXED_DOCUMENT_VOLUME_FIELD.getPreferredName(), indexedDocumentVolume);
                builder.field(ConnectorSyncJob.TOTAL_DOCUMENT_COUNT_FIELD.getPreferredName(), totalDocumentCount);
                builder.field(ConnectorSyncJob.LAST_SEEN_FIELD.getPreferredName(), lastSeen);
                builder.field(ConnectorSyncJob.METADATA_FIELD.getPreferredName(), metadata);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(connectorSyncJobId);
            out.writeLong(deletedDocumentCount);
            out.writeLong(indexedDocumentCount);
            out.writeLong(indexedDocumentVolume);
            out.writeOptionalLong(totalDocumentCount);
            out.writeOptionalInstant(lastSeen);
            out.writeGenericMap(metadata);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(connectorSyncJobId, request.connectorSyncJobId)
                && Objects.equals(deletedDocumentCount, request.deletedDocumentCount)
                && Objects.equals(indexedDocumentCount, request.indexedDocumentCount)
                && Objects.equals(indexedDocumentVolume, request.indexedDocumentVolume)
                && Objects.equals(totalDocumentCount, request.totalDocumentCount)
                && Objects.equals(lastSeen, request.lastSeen)
                && Objects.equals(metadata, request.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                connectorSyncJobId,
                deletedDocumentCount,
                indexedDocumentCount,
                indexedDocumentVolume,
                totalDocumentCount,
                lastSeen,
                metadata
            );
        }
    }

}
