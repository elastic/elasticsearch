/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MonitoringMigrateAlertsResponse extends ActionResponse implements ToXContentObject {

    private final List<ExporterMigrationResult> exporters;

    public MonitoringMigrateAlertsResponse(List<ExporterMigrationResult> exporters) {
        this.exporters = exporters;
    }

    public MonitoringMigrateAlertsResponse(StreamInput in) throws IOException {
        this.exporters = in.readCollectionAsList(ExporterMigrationResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(exporters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        return builder.startObject().array("exporters", exporters).endObject();
    }

    public List<ExporterMigrationResult> getExporters() {
        return exporters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MonitoringMigrateAlertsResponse response = (MonitoringMigrateAlertsResponse) o;
        return Objects.equals(exporters, response.exporters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exporters);
    }

    @Override
    public String toString() {
        return "MonitoringMigrateAlertsResponse{" + "exporters=" + exporters + '}';
    }

    public static class ExporterMigrationResult implements Writeable, ToXContentObject {

        private final String name;
        private final String type;
        private final boolean migrationComplete;
        private final Exception reason;

        public ExporterMigrationResult(String name, String type, boolean migrationComplete, Exception reason) {
            this.name = name;
            this.type = type;
            this.migrationComplete = migrationComplete;
            this.reason = reason;
        }

        public ExporterMigrationResult(StreamInput in) throws IOException {
            this.name = in.readString();
            this.type = in.readString();
            this.migrationComplete = in.readBoolean();
            this.reason = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeBoolean(migrationComplete);
            out.writeException(reason);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", name);
                builder.field("type", type);
                builder.field("migration_complete", migrationComplete);
                if (reason != null) {
                    builder.startObject("reason");
                    ElasticsearchException.generateThrowableXContent(builder, params, reason);
                    builder.endObject();
                }
            }
            return builder.endObject();
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public boolean isMigrationComplete() {
            return migrationComplete;
        }

        @Nullable
        public Exception getReason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExporterMigrationResult that = (ExporterMigrationResult) o;
            return migrationComplete == that.migrationComplete && Objects.equals(name, that.name) && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, migrationComplete);
        }

        @Override
        public String toString() {
            return "ExporterMigrationResult{"
                + "name='"
                + name
                + '\''
                + ", type='"
                + type
                + '\''
                + ", migrationComplete="
                + migrationComplete
                + ", reason="
                + reason
                + '}';
        }
    }
}
