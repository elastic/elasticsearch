/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.monitoring.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class MonitoringMigrateAlertsResponse extends ActionResponse {

    private final List<ExporterMigrationResult> exporters;

    public MonitoringMigrateAlertsResponse(List<ExporterMigrationResult> exporters) {
        this.exporters = exporters;
    }

    public MonitoringMigrateAlertsResponse(StreamInput in) throws IOException {
        super(in);
        this.exporters = in.readList(ExporterMigrationResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(exporters);
    }

    public List<ExporterMigrationResult> getExporters() {
        return exporters;
    }

    public static class ExporterMigrationResult implements Writeable, ToXContentObject {

        private final String name;
        private final String type;
        private final boolean migrationComplete;
        private final String reason;

        public ExporterMigrationResult(String name, String type, boolean migrationComplete, String reason) {
            this.name = name;
            this.type = type;
            this.migrationComplete = migrationComplete;
            this.reason = reason;
        }

        public ExporterMigrationResult(StreamInput in) throws IOException {
            this.name = in.readString();
            this.type = in.readString();
            this.migrationComplete = in.readBoolean();
            this.reason = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeBoolean(migrationComplete);
            out.writeOptionalString(reason);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("name", name);
                builder.field("type", type);
                builder.field("migration_complete", migrationComplete);
                if (reason != null) {
                    builder.field("reason", reason);
                }
            }
            return builder.endObject();
        }
    }
}
