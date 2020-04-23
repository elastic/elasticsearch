/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contains the information on what V2 templates would match a given index.
 */
public class SimulateIndexTemplateResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField OVERLAPPING_V1_TEMPLATES = new ParseField("overlapping_v1_templates");

    @Nullable
    // the resolved settings, mappings and aliases for the matched templates, if any
    private Template resolvedTemplate;

    @Nullable
    // a map of v1 template names and their index patterns that would overlap when matching the given index name
    private Map<String, List<String>> overlappingV1Templates;

    public SimulateIndexTemplateResponse(@Nullable Template resolvedTemplate, @Nullable Map<String, List<String>> overlappingV1Templates) {
        this.resolvedTemplate = resolvedTemplate;
        this.overlappingV1Templates = overlappingV1Templates;
    }

    public SimulateIndexTemplateResponse(StreamInput in) throws IOException {
        super(in);
        resolvedTemplate = in.readOptionalWriteable(Template::new);
        if (in.readBoolean()) {
            int conflictingV1TemplatesCount = in.readInt();
            overlappingV1Templates = new HashMap<>(conflictingV1TemplatesCount, 1L);
            for (int i = 0; i < conflictingV1TemplatesCount; i++) {
                String templateName = in.readString();
                overlappingV1Templates.put(templateName, in.readStringList());
            }
        } else {
            this.overlappingV1Templates = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(resolvedTemplate);
        if (overlappingV1Templates != null) {
            out.writeBoolean(true);
            out.writeInt(overlappingV1Templates.size());
            for (Map.Entry<String, List<String>> entry : overlappingV1Templates.entrySet()) {
                out.writeString(entry.getKey());
                out.writeStringCollection(entry.getValue());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.resolvedTemplate != null) {
            builder.field(TEMPLATE.getPreferredName(), this.resolvedTemplate);
        }
        if (this.overlappingV1Templates != null) {
            builder.field(OVERLAPPING_V1_TEMPLATES.getPreferredName(), overlappingV1Templates);
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
            && Objects.deepEquals(overlappingV1Templates, that.overlappingV1Templates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolvedTemplate, overlappingV1Templates);
    }

    @Override
    public String toString() {
        return "SimulateIndexTemplateResponse{" + "resolved template=" + resolvedTemplate + ", overlapping v1 templates="
            + String.join("|", overlappingV1Templates.keySet()) + "}";
    }
}
