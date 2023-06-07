/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class GetComposableIndexTemplateAction extends ActionType<GetComposableIndexTemplateAction.Response> {

    public static final GetComposableIndexTemplateAction INSTANCE = new GetComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/get";

    private GetComposableIndexTemplateAction() {
        super(NAME, GetComposableIndexTemplateAction.Response::new);
    }

    /**
     * Request that to retrieve one or more index templates
     */
    public static class Request extends MasterNodeReadRequest<Request> {

        @Nullable
        private final String name;
        private boolean includeDefaults;

        /**
         * @param name A template name or pattern, or {@code null} to retrieve all templates.
         */
        public Request(@Nullable String name) {
            if (name != null && name.contains(",")) {
                throw new IllegalArgumentException("template name may not contain ','");
            }
            this.name = name;
            this.includeDefaults = false;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readOptionalString();
            includeDefaults = false;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        public void includeDefaults(boolean includeDefaults) {
            this.includeDefaults = includeDefaults;
        }

        public boolean includeDefaults() {
            return includeDefaults;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * The name of the index templates.
         */
        public String name() {
            return this.name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(name, other.name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField NAME = new ParseField("name");
        public static final ParseField INDEX_TEMPLATES = new ParseField("index_templates");
        public static final ParseField INDEX_TEMPLATE = new ParseField("index_template");

        private final Map<String, ComposableIndexTemplate> indexTemplates;
        private final RolloverConfiguration rolloverConfiguration;

        public Response(StreamInput in) throws IOException {
            super(in);
            indexTemplates = in.readMap(StreamInput::readString, ComposableIndexTemplate::new);
            rolloverConfiguration = null;
        }

        public Response(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.indexTemplates = indexTemplates;
            this.rolloverConfiguration = null;
        }

        public Response(Map<String, ComposableIndexTemplate> indexTemplates, RolloverConfiguration rolloverConfiguration) {
            this.indexTemplates = indexTemplates;
            this.rolloverConfiguration = rolloverConfiguration;
        }

        public Map<String, ComposableIndexTemplate> indexTemplates() {
            return indexTemplates;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indexTemplates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetComposableIndexTemplateAction.Response that = (GetComposableIndexTemplateAction.Response) o;
            return Objects.equals(indexTemplates, that.indexTemplates) && Objects.equals(rolloverConfiguration, that.rolloverConfiguration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTemplates, rolloverConfiguration);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(INDEX_TEMPLATES.getPreferredName());
            for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : this.indexTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), indexTemplate.getKey());
                builder.field(INDEX_TEMPLATE.getPreferredName());
                indexTemplate.getValue().toXContent(builder, params, rolloverConfiguration);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }

}
