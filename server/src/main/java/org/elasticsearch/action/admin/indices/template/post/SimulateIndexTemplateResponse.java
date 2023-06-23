/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
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
    private Template resolvedTemplate;

    @Nullable
    // a map of template names and their index patterns that would overlap when matching the given index name
    private Map<String, List<String>> overlappingTemplates;

    @Nullable
    private RolloverConfiguration rolloverConfiguration = null;

    public SimulateIndexTemplateResponse(@Nullable Template resolvedTemplate, @Nullable Map<String, List<String>> overlappingTemplates) {
        this(resolvedTemplate, overlappingTemplates, null);
    }

    public SimulateIndexTemplateResponse(
        @Nullable Template resolvedTemplate,
        @Nullable Map<String, List<String>> overlappingTemplates,
        @Nullable RolloverConfiguration rolloverConfiguration
    ) {
        this.resolvedTemplate = resolvedTemplate;
        this.overlappingTemplates = overlappingTemplates;
        this.rolloverConfiguration = rolloverConfiguration;
    }

    public SimulateIndexTemplateResponse(StreamInput in) throws IOException {
        super(in);
        resolvedTemplate = in.readOptionalWriteable(Template::new);
        if (in.readBoolean()) {
            int overlappingTemplatesCount = in.readInt();
            overlappingTemplates = Maps.newMapWithExpectedSize(overlappingTemplatesCount);
            for (int i = 0; i < overlappingTemplatesCount; i++) {
                String templateName = in.readString();
                overlappingTemplates.put(templateName, in.readStringList());
            }
        } else {
            this.overlappingTemplates = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            rolloverConfiguration = in.readOptionalWriteable(RolloverConfiguration::new);
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_007)) {
            out.writeOptionalWriteable(rolloverConfiguration);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.resolvedTemplate != null) {
            builder.field(TEMPLATE.getPreferredName());
            this.resolvedTemplate.toXContent(builder, params, rolloverConfiguration);
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
            && Objects.deepEquals(overlappingTemplates, that.overlappingTemplates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolvedTemplate, overlappingTemplates);
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
