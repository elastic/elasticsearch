/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contains the information on what V2 templates would match a given index.
 */
public class SimulateIndexTemplateResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField OVERLAPPING = new ParseField("overlapping");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");

    @Nullable
    // the resolved settings, mappings and aliases for the matched templates, if any
    private final Template resolvedTemplate;

    @Nullable
    // a map of template names and their index patterns that would overlap when matching the given index name
    private final Map<String, List<String>> overlappingTemplates;

    @Nullable
    private final RolloverConfiguration rolloverConfiguration;
    @Nullable
    private final DataStreamGlobalRetention globalRetention;

    public SimulateIndexTemplateResponse(
        @Nullable Template resolvedTemplate,
        @Nullable Map<String, List<String>> overlappingTemplates,
        DataStreamGlobalRetention globalRetention
    ) {
        this(resolvedTemplate, overlappingTemplates, null, globalRetention);
    }

    public SimulateIndexTemplateResponse(
        @Nullable Template resolvedTemplate,
        @Nullable Map<String, List<String>> overlappingTemplates,
        @Nullable RolloverConfiguration rolloverConfiguration,
        @Nullable DataStreamGlobalRetention globalRetention
    ) {
        this.resolvedTemplate = resolvedTemplate;
        this.overlappingTemplates = overlappingTemplates;
        this.rolloverConfiguration = rolloverConfiguration;
        this.globalRetention = globalRetention;
    }

    public SimulateIndexTemplateResponse(StreamInput in) throws IOException {
        super(in);
        resolvedTemplate = in.readOptionalWriteable(Template::new);
        if (in.readBoolean()) {
            int overlappingTemplatesCount = in.readInt();
            overlappingTemplates = Maps.newMapWithExpectedSize(overlappingTemplatesCount);
            for (int i = 0; i < overlappingTemplatesCount; i++) {
                String templateName = in.readString();
                overlappingTemplates.put(templateName, in.readStringCollectionAsList());
            }
        } else {
            this.overlappingTemplates = null;
        }
        rolloverConfiguration = in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)
            ? in.readOptionalWriteable(RolloverConfiguration::new)
            : null;
        globalRetention = in.getTransportVersion().onOrAfter(TransportVersions.USE_DATA_STREAM_GLOBAL_RETENTION)
            ? in.readOptionalWriteable(DataStreamGlobalRetention::read)
            : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(resolvedTemplate);
        if (overlappingTemplates != null) {
            out.writeBoolean(true);
            out.writeInt(overlappingTemplates.size());
            for (Map.Entry<String, List<String>> entry : overlappingTemplates.entrySet()) {
                out.writeString(entry.getKey());
                out.writeStringCollection(entry.getValue());
            }
        } else {
            out.writeBoolean(false);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            out.writeOptionalWriteable(rolloverConfiguration);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.USE_DATA_STREAM_GLOBAL_RETENTION)) {
            out.writeOptionalWriteable(globalRetention);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Params withEffectiveRetentionParams = new DelegatingMapParams(DataStreamLifecycle.INCLUDE_EFFECTIVE_RETENTION_PARAMS, params);
        builder.startObject();
        if (this.resolvedTemplate != null) {
            builder.field(TEMPLATE.getPreferredName());
            this.resolvedTemplate.toXContent(builder, withEffectiveRetentionParams, rolloverConfiguration, globalRetention);
        }
        if (this.overlappingTemplates != null) {
            builder.startArray(OVERLAPPING.getPreferredName());
            for (Map.Entry<String, List<String>> entry : overlappingTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), entry.getKey());
                builder.stringListField(INDEX_PATTERNS.getPreferredName(), entry.getValue());
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimulateIndexTemplateResponse that = (SimulateIndexTemplateResponse) o;
        return Objects.equals(resolvedTemplate, that.resolvedTemplate)
            && Objects.deepEquals(overlappingTemplates, that.overlappingTemplates)
            && Objects.equals(rolloverConfiguration, that.rolloverConfiguration)
            && Objects.equals(globalRetention, that.globalRetention);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolvedTemplate, overlappingTemplates, rolloverConfiguration, globalRetention);
    }

    @Override
    public String toString() {
        return "SimulateIndexTemplateResponse{"
            + "resolved template="
            + resolvedTemplate
            + ", overlapping templates="
            + String.join("|", overlappingTemplates.keySet())
            + "}";
    }
}
