/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
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
        super(NAME);
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
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
            if (name != null && name.contains(",")) {
                throw new IllegalArgumentException("template name may not contain ','");
            }
            this.name = name;
            this.includeDefaults = false;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readOptionalString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                includeDefaults = in.readBoolean();
            } else {
                includeDefaults = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeBoolean(includeDefaults);
            }
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
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;
        @Nullable
        private final DataStreamGlobalRetention globalRetention;

        public Response(StreamInput in) throws IOException {
            super(in);
            indexTemplates = in.readMap(ComposableIndexTemplate::new);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                rolloverConfiguration = in.readOptionalWriteable(RolloverConfiguration::new);
            } else {
                rolloverConfiguration = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                globalRetention = in.readOptionalWriteable(DataStreamGlobalRetention::read);
            } else {
                globalRetention = null;
            }
        }

        public Response(Map<String, ComposableIndexTemplate> indexTemplates, @Nullable DataStreamGlobalRetention globalRetention) {
            this(indexTemplates, null, globalRetention);
        }

        public Response(
            Map<String, ComposableIndexTemplate> indexTemplates,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention globalRetention
        ) {
            this.indexTemplates = indexTemplates;
            this.rolloverConfiguration = rolloverConfiguration;
            this.globalRetention = globalRetention;
        }

        public Map<String, ComposableIndexTemplate> indexTemplates() {
            return indexTemplates;
        }

        public DataStreamGlobalRetention getGlobalRetention() {
            return globalRetention;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indexTemplates, StreamOutput::writeWriteable);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeOptionalWriteable(rolloverConfiguration);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
                out.writeOptionalWriteable(globalRetention);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetComposableIndexTemplateAction.Response that = (GetComposableIndexTemplateAction.Response) o;
            return Objects.equals(indexTemplates, that.indexTemplates)
                && Objects.equals(rolloverConfiguration, that.rolloverConfiguration)
                && Objects.equals(globalRetention, that.globalRetention);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexTemplates, rolloverConfiguration, globalRetention);
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
