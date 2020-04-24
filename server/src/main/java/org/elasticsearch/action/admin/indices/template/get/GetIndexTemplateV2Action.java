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
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
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

public class GetIndexTemplateV2Action extends ActionType<GetIndexTemplateV2Action.Response> {

    public static final GetIndexTemplateV2Action INSTANCE = new GetIndexTemplateV2Action();
    public static final String NAME = "indices:admin/index_template/get";

    private GetIndexTemplateV2Action() {
        super(NAME, GetIndexTemplateV2Action.Response::new);
    }

    /**
     * Request that to retrieve one or more index templates
     */
    public static class Request extends MasterNodeReadRequest<Request> {

        @Nullable
        private String name;

        public Request() { }

        public Request(@Nullable String name) {
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
         * Sets the name of the index template.
         */
        public Request name(String name) {
            this.name = name;
            return this;
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

        private final Map<String, IndexTemplateV2> indexTemplates;

        public Response(StreamInput in) throws IOException {
            super(in);
            int size = in.readVInt();
            indexTemplates = new HashMap<>();
            for (int i = 0 ; i < size ; i++) {
                indexTemplates.put(in.readString(), new IndexTemplateV2(in));
            }
        }

        public Response(Map<String, IndexTemplateV2> indexTemplates) {
            this.indexTemplates = indexTemplates;
        }

        public Map<String, IndexTemplateV2> indexTemplates() {
            return indexTemplates;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(indexTemplates.size());
            for (Map.Entry<String, IndexTemplateV2> indexTemplate : indexTemplates.entrySet()) {
                out.writeString(indexTemplate.getKey());
                indexTemplate.getValue().writeTo(out);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetIndexTemplateV2Action.Response that = (GetIndexTemplateV2Action.Response) o;
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
            for (Map.Entry<String, IndexTemplateV2> indexTemplate : this.indexTemplates.entrySet()) {
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
