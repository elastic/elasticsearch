/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MigrateToDataTiersRequest extends AcknowledgedRequest<MigrateToDataTiersRequest> {

    private static final ParseField LEGACY_TEMPLATE_TO_DELETE = new ParseField("legacy_template_to_delete");
    private static final ParseField NODE_ATTRIBUTE_NAME = new ParseField("node_attribute");

    public interface Factory {
        MigrateToDataTiersRequest create(@Nullable String legacyTemplateToDelete, @Nullable String nodeAttributeName);
    }

    public static final ConstructingObjectParser<MigrateToDataTiersRequest, Factory> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        (a, factory) -> factory.create((String) a[0], (String) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), LEGACY_TEMPLATE_TO_DELETE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NODE_ATTRIBUTE_NAME);
    }

    /**
     * Represents the name of node attribute used for index shard allocation filtering (usually `data`)
     */
    @Nullable
    private final String nodeAttributeName;

    /**
     * Represents the name of the legacy (v1) index template to delete.
     */
    @Nullable
    private final String legacyTemplateToDelete;
    private boolean dryRun = false;

    public static MigrateToDataTiersRequest parse(Factory factory, XContentParser parser) throws IOException {
        return PARSER.parse(parser, factory);
    }

    public MigrateToDataTiersRequest(
        TimeValue masterNodeTimeout,
        @Nullable String legacyTemplateToDelete,
        @Nullable String nodeAttributeName
    ) {
        super(masterNodeTimeout, DEFAULT_ACK_TIMEOUT);
        this.legacyTemplateToDelete = legacyTemplateToDelete;
        this.nodeAttributeName = nodeAttributeName;
    }

    public MigrateToDataTiersRequest(StreamInput in) throws IOException {
        super(in);
        dryRun = in.readBoolean();
        legacyTemplateToDelete = in.readOptionalString();
        nodeAttributeName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(dryRun);
        out.writeOptionalString(legacyTemplateToDelete);
        out.writeOptionalString(nodeAttributeName);
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public String getNodeAttributeName() {
        return nodeAttributeName;
    }

    public String getLegacyTemplateToDelete() {
        return legacyTemplateToDelete;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MigrateToDataTiersRequest that = (MigrateToDataTiersRequest) o;
        return dryRun == that.dryRun
            && Objects.equals(nodeAttributeName, that.nodeAttributeName)
            && Objects.equals(legacyTemplateToDelete, that.legacyTemplateToDelete);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeAttributeName, legacyTemplateToDelete, dryRun);
    }
}
