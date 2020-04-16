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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonMap;

public class GetIndexTemplatesResponse extends ActionResponse implements ToXContentObject {

    private final List<IndexTemplateMetadata> indexTemplates;

    public GetIndexTemplatesResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        indexTemplates = new ArrayList<>();
        for (int i = 0 ; i < size ; i++) {
            indexTemplates.add(IndexTemplateMetadata.readFrom(in));
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetIndexTemplatesResponse that = (GetIndexTemplatesResponse) o;
        return Objects.equals(indexTemplates, that.indexTemplates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexTemplates);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        params = new ToXContent.DelegatingMapParams(singletonMap("reduce_mappings", "true"), params);

        builder.startObject();
        for (IndexTemplateMetadata indexTemplateMetadata : getIndexTemplates()) {
            IndexTemplateMetadata.Builder.toXContent(indexTemplateMetadata, builder, params);
        }
        builder.endObject();
        return builder;
    }

}
