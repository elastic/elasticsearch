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

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Action to retrieve one or more component templates
 */
public class GetComponentTemplateAction extends ActionType<GetComponentTemplateAction.Response> {

    public static final GetComponentTemplateAction INSTANCE = new GetComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/get";

    private GetComponentTemplateAction() {
        super(NAME, GetComponentTemplateAction.Response::new);
    }

    /**
     * Request that to retrieve one or more component templates
     */
    public static class Request extends MasterNodeReadRequest<Request> {

        @Nullable
        private String name;

        public Request() { }

        public Request(String name) {
            this.name = name;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * Sets the name of the component templates.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        /**
         * The name of the component templates.
         */
        public String name() {
            return this.name;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField NAME = new ParseField("name");
        public static final ParseField COMPONENT_TEMPLATES = new ParseField("component_templates");
        public static final ParseField COMPONENT_TEMPLATE = new ParseField("component_template");

        private final Map<String, ComponentTemplate> componentTemplates;

        public Response(StreamInput in) throws IOException {
            super(in);
            int size = in.readVInt();
            componentTemplates = new HashMap<>();
            for (int i = 0 ; i < size ; i++) {
                componentTemplates.put(in.readString(), new ComponentTemplate(in));
            }
        }

        public Response(Map<String, ComponentTemplate> componentTemplates) {
            this.componentTemplates = componentTemplates;
        }

        public Map<String, ComponentTemplate> getComponentTemplates() {
            return componentTemplates;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(componentTemplates.size());
            for (Map.Entry<String, ComponentTemplate> componentTemplate : componentTemplates.entrySet()) {
                out.writeString(componentTemplate.getKey());
                componentTemplate.getValue().writeTo(out);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(componentTemplates, that.componentTemplates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(componentTemplates);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(COMPONENT_TEMPLATES.getPreferredName());
            for (Map.Entry<String, ComponentTemplate> componentTemplate : this.componentTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), componentTemplate.getKey());
                builder.field(COMPONENT_TEMPLATE.getPreferredName(), componentTemplate.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }

}
