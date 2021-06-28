/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.List;

public class MigrateToDataTiersResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField REMOVED_LEGACY_TEMPLATE = new ParseField("removed_legacy_template");
    public static final ParseField MIGRATED_INDICES = new ParseField("migrated_indices");
    public static final ParseField MIGRATED_ILM_POLICIES = new ParseField("migrated_ilm_policies");

    @Nullable
    private final String removedIndexTemplateName;
    private final List<String> migratedPolicies;
    private final List<String> migratedIndices;

    public MigrateToDataTiersResponse(@Nullable String removedIndexTemplateName, List<String> migratedPolicies,
                                      List<String> migratedIndices) {
        this.removedIndexTemplateName = removedIndexTemplateName;
        this.migratedPolicies = migratedPolicies;
        this.migratedIndices = migratedIndices;
    }

    public MigrateToDataTiersResponse(StreamInput in) throws IOException {
        super(in);
        removedIndexTemplateName = in.readOptionalString();
        migratedPolicies = in.readStringList();
        migratedIndices = in.readStringList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.removedIndexTemplateName != null) {
            builder.field(REMOVED_LEGACY_TEMPLATE.getPreferredName(), this.removedIndexTemplateName);
        }
        if (migratedPolicies.size() > 0) {
            builder.startArray(MIGRATED_ILM_POLICIES.getPreferredName());
            for (String policy : migratedPolicies) {
                builder.value(policy);
            }
            builder.endArray();
        }
        if (migratedIndices.size() > 0) {
            builder.startArray(MIGRATED_INDICES.getPreferredName());
            for (String index : migratedIndices) {
                builder.value(index);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(removedIndexTemplateName);
        out.writeStringCollection(migratedPolicies);
        out.writeStringCollection(migratedIndices);
    }
}
