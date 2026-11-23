/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

/**
 * Serializes records into the body of one batch request of the Usage API: a JSON array of usage records (the OpenAPI specification
 * of {@code POST /api/v1/usage}, schema v2 of the usage record). The field names and places are fixed by that specification, and
 * the committed test vectors pin them, so that other implementations of the same records can be checked against the same files.
 * <p>
 * {@code source.provider} and {@code source.region} are written although the OpenAPI text says the API fills them: for requests
 * authenticated with a client certificate the Usage API answers 403 to a record without them, and it does not fill them itself.
 * <p>
 * Hosted records identify their cluster in {@code source.metadata.cluster_id}: billing drops hosted records without it.
 */
public final class UsageRecordJson {

    /** The only kind of instance group a node of a hosted deployment reports for. */
    static final String INSTANCE_GROUP_TYPE = "stateful_deployment";

    private UsageRecordJson() {}

    public static BytesReference toJson(List<UsageRecord> records) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startArray();
            for (UsageRecord record : records) {
                writeRecord(builder, record);
            }
            builder.endArray();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeRecord(XContentBuilder builder, UsageRecord record) throws IOException {
        builder.startObject();
        builder.field("id", record.id());
        builder.field("usage_timestamp", HostedUsageRecordBuilder.TIMESTAMP_FORMAT.format(record.usageTimestamp()));
        builder.field("creation_timestamp", HostedUsageRecordBuilder.TIMESTAMP_FORMAT.format(record.creationTimestamp()));

        builder.startObject("usage");
        builder.field("type", record.usageType());
        builder.field("quantity", record.quantityMillis());
        builder.field("period_seconds", record.periodSeconds());
        if (record.exemption() != null) {
            builder.startObject("exemption");
            builder.field("exempt", true);
            builder.field("reason", record.exemption().reason());
            builder.endObject();
        }
        if (record.metadata().isEmpty() == false) {
            builder.startObject("metadata");
            for (Map.Entry<String, String> entry : record.metadata().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        builder.endObject();

        SourceIdentity source = record.source();
        builder.startObject("source");
        // must agree with the prefix of the record id, which the builder derives from the same constant
        builder.field("id", HostedUsageRecordBuilder.SOURCE_ID_PREFIX + source.nodeId());
        builder.field("instance_group_id", source.deploymentId());
        builder.field("instance_group_type", INSTANCE_GROUP_TYPE);
        builder.field("provider", source.provider());
        builder.field("region", source.region());
        builder.startObject("metadata");
        builder.field("cluster_id", source.clusterId());
        builder.endObject();
        builder.endObject();

        builder.endObject();
    }
}
