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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;
import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;

public class GetIndexTemplatesResponse extends ActionResponse implements ToXContentObject {

    private final List<IndexTemplateMetadata> indexTemplates;

    public GetIndexTemplatesResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        indexTemplates = new ArrayList<>();
        for (int i = 0 ; i < size ; i++) {
            indexTemplates.add(0, IndexTemplateMetadata.readFrom(in));
        }
    }

    public GetIndexTemplatesResponse(List<IndexTemplateMetadata> indexTemplates) {
        this.indexTemplates = indexTemplates;
    }

    public List<IndexTemplateMetadata> getIndexTemplates() {
        return indexTemplates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(indexTemplates.size());
        for (IndexTemplateMetadata indexTemplate : indexTemplates) {
            indexTemplate.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        params = new ToXContent.DelegatingMapParams(singletonMap("reduce_mappings", "true"), params);

        boolean includeTypeName = params.paramAsBoolean(INCLUDE_TYPE_NAME_PARAMETER,
            DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        builder.startObject();
        for (IndexTemplateMetadata indexTemplateMetadata : getIndexTemplates()) {
            if (includeTypeName) {
                IndexTemplateMetadata.Builder.toXContentWithTypes(indexTemplateMetadata, builder, params);
            } else {
                IndexTemplateMetadata.Builder.toXContent(indexTemplateMetadata, builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    public static GetIndexTemplatesResponse fromXContent(XContentParser parser) throws IOException {
        final List<IndexTemplateMetadata> templates = new ArrayList<>();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                final IndexTemplateMetadata templateMetadata = IndexTemplateMetadata.Builder.fromXContent(parser, parser.currentName());
                templates.add(templateMetadata);
            }
        }
        return new GetIndexTemplatesResponse(templates);
    }
}
