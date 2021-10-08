/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
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

        private final String[] names;

        public Request(String... names) {
            if (Arrays.stream(Objects.requireNonNull(names)).anyMatch(Objects::isNull)) {
                throw new NullPointerException("names");
            }
            this.names = names;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                names = in.readStringArray();
            } else {
                final String optionalName = in.readOptionalString();
                names = optionalName == null ? Strings.EMPTY_ARRAY : new String[]{optionalName};
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeStringArray(names);
            } else if (names.length == 0) {
                out.writeOptionalString(null);
            } else if (names.length == 1) {
                out.writeOptionalString(names[0]);
            } else {
                throw new IllegalArgumentException("action [" + NAME + "] does not support multiple names in version " + out.getVersion());
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        /**
         * The name of the index templates.
         */
        public String[] names() {
            return this.names;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
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
            return Arrays.equals(names, other.names);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField NAME = new ParseField("name");
        public static final ParseField INDEX_TEMPLATES = new ParseField("index_templates");
        public static final ParseField INDEX_TEMPLATE = new ParseField("index_template");

        private final Map<String, ComposableIndexTemplate> indexTemplates;

        public Response(StreamInput in) throws IOException {
            super(in);
            indexTemplates = in.readMap(StreamInput::readString, ComposableIndexTemplate::new);
        }

        public Response(Map<String, ComposableIndexTemplate> indexTemplates) {
            this.indexTemplates = indexTemplates;
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
            return Objects.equals(indexTemplates, that.indexTemplates);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTemplates);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(INDEX_TEMPLATES.getPreferredName());
            for (Map.Entry<String, ComposableIndexTemplate> indexTemplate : this.indexTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), indexTemplate.getKey());
                builder.field(INDEX_TEMPLATE.getPreferredName(), indexTemplate.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }

}
