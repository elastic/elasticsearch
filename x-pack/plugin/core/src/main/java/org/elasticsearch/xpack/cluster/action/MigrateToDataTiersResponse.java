/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MigrateToDataTiersResponse extends ActionResponse implements ToXContentObject {

    public static final ParseField REMOVED_LEGACY_TEMPLATE = new ParseField("removed_legacy_template");
    public static final ParseField MIGRATED_INDICES = new ParseField("migrated_indices");
    public static final ParseField MIGRATED_ILM_POLICIES = new ParseField("migrated_ilm_policies");
    public static final ParseField MIGRATED_LEGACY_TEMPLATES = new ParseField("migrated_legacy_templates");
    public static final ParseField MIGRATED_COMPOSABLE_TEMPLATES = new ParseField("migrated_composable_templates");
    public static final ParseField MIGRATED_COMPONENT_TEMPLATES = new ParseField("migrated_component_templates");
    public static final ParseField DRY_RUN = new ParseField("dry_run");

    @Nullable
    private final String removedIndexTemplateName;
    private final List<String> migratedPolicies;
    private final List<String> migratedIndices;
    private final boolean dryRun;
    private final List<String> migratedLegacyTemplates;
    private final List<String> migratedComposableTemplates;
    private final List<String> migratedComponentTemplates;

    public MigrateToDataTiersResponse(
        @Nullable String removedIndexTemplateName,
        List<String> migratedPolicies,
        List<String> migratedIndices,
        List<String> migratedLegacyTemplates,
        List<String> migratedComposableTemplates,
        List<String> migratedComponentTemplates,
        boolean dryRun
    ) {
        this.removedIndexTemplateName = removedIndexTemplateName;
        this.migratedPolicies = migratedPolicies;
        this.migratedIndices = migratedIndices;
        this.migratedLegacyTemplates = migratedLegacyTemplates;
        this.migratedComposableTemplates = migratedComposableTemplates;
        this.migratedComponentTemplates = migratedComponentTemplates;
        this.dryRun = dryRun;
    }

    public MigrateToDataTiersResponse(StreamInput in) throws IOException {
        super(in);
        removedIndexTemplateName = in.readOptionalString();
        migratedPolicies = in.readStringList();
        migratedIndices = in.readStringList();
        dryRun = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_7_17_0)) {
            migratedLegacyTemplates = in.readStringList();
            migratedComposableTemplates = in.readStringList();
            migratedComponentTemplates = in.readStringList();
        } else {
            migratedLegacyTemplates = List.of();
            migratedComposableTemplates = List.of();
            migratedComponentTemplates = List.of();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DRY_RUN.getPreferredName(), dryRun);
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
        if (migratedLegacyTemplates.size() > 0) {
            builder.startArray(MIGRATED_LEGACY_TEMPLATES.getPreferredName());
            for (String legacyTemplate : migratedLegacyTemplates) {
                builder.value(legacyTemplate);
            }
            builder.endArray();
        }
        if (migratedComposableTemplates.size() > 0) {
            builder.startArray(MIGRATED_COMPOSABLE_TEMPLATES.getPreferredName());
            for (String composableTemplate : migratedComposableTemplates) {
                builder.value(composableTemplate);
            }
            builder.endArray();
        }
        if (migratedComponentTemplates.size() > 0) {
            builder.startArray(MIGRATED_COMPONENT_TEMPLATES.getPreferredName());
            for (String componentTemplate : migratedComponentTemplates) {
                builder.value(componentTemplate);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    public String getRemovedIndexTemplateName() {
        return removedIndexTemplateName;
    }

    public List<String> getMigratedPolicies() {
        return migratedPolicies;
    }

    public List<String> getMigratedIndices() {
        return migratedIndices;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public List<String> getMigratedLegacyTemplates() {
        return migratedLegacyTemplates;
    }

    public List<String> getMigratedComposableTemplates() {
        return migratedComposableTemplates;
    }

    public List<String> getMigratedComponentTemplates() {
        return migratedComponentTemplates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(removedIndexTemplateName);
        out.writeStringCollection(migratedPolicies);
        out.writeStringCollection(migratedIndices);
        out.writeBoolean(dryRun);
        if (out.getVersion().onOrAfter(Version.V_7_17_0)) {
            out.writeStringCollection(migratedLegacyTemplates);
            out.writeStringCollection(migratedComposableTemplates);
            out.writeStringCollection(migratedComponentTemplates);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MigrateToDataTiersResponse that = (MigrateToDataTiersResponse) o;
        return dryRun == that.dryRun
            && Objects.equals(removedIndexTemplateName, that.removedIndexTemplateName)
            && Objects.equals(migratedPolicies, that.migratedPolicies)
            && Objects.equals(migratedIndices, that.migratedIndices)
            && Objects.equals(migratedLegacyTemplates, that.migratedLegacyTemplates)
            && Objects.equals(migratedComposableTemplates, that.migratedComposableTemplates)
            && Objects.equals(migratedComponentTemplates, that.migratedComponentTemplates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            removedIndexTemplateName,
            migratedPolicies,
            migratedIndices,
            dryRun,
            migratedLegacyTemplates,
            migratedComposableTemplates,
            migratedComponentTemplates
        );
    }
}
