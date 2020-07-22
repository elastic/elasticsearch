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
    private static final ParseField OVERLAPPING = new ParseField("overlapping");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");

    @Nullable
    // the resolved settings, mappings and aliases for the matched templates, if any
    private Template resolvedTemplate;

    @Nullable
    // a map of template names and their index patterns that would overlap when matching the given index name
    private Map<String, List<String>> overlappingTemplates;

    public SimulateIndexTemplateResponse(@Nullable Template resolvedTemplate, @Nullable Map<String, List<String>> overlappingTemplates) {
        this.resolvedTemplate = resolvedTemplate;
        this.overlappingTemplates = overlappingTemplates;
    }

    public SimulateIndexTemplateResponse(StreamInput in) throws IOException {
        super(in);
        resolvedTemplate = in.readOptionalWriteable(Template::new);
        if (in.readBoolean()) {
            int overlappingTemplatesCount = in.readInt();
            overlappingTemplates = new HashMap<>(overlappingTemplatesCount, 1L);
            for (int i = 0; i < overlappingTemplatesCount; i++) {
                String templateName = in.readString();
                overlappingTemplates.put(templateName, in.readStringList());
            }
        } else {
            this.overlappingTemplates = null;
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (this.resolvedTemplate != null) {
            builder.field(TEMPLATE.getPreferredName(), this.resolvedTemplate);
        }
        if (this.overlappingTemplates != null) {
            builder.startArray(OVERLAPPING.getPreferredName());
            for (Map.Entry<String, List<String>> entry : overlappingTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), entry.getKey());
                builder.field(INDEX_PATTERNS.getPreferredName(), entry.getValue());
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
        return "SimulateIndexTemplateResponse{" + "resolved template=" + resolvedTemplate + ", overlapping templates="
            + String.join("|", overlappingTemplates.keySet()) + "}";
    }
}
