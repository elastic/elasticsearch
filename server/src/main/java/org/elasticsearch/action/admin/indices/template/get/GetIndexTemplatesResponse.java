/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonMap;

public class GetIndexTemplatesResponse extends ActionResponse implements ToXContentObject {

    private final List<IndexTemplateMetadata> indexTemplates;

    public GetIndexTemplatesResponse(StreamInput in) throws IOException {
        super(in);
        indexTemplates = in.readCollectionAsList(IndexTemplateMetadata::readFrom);
    }

    public GetIndexTemplatesResponse(List<IndexTemplateMetadata> indexTemplates) {
        this.indexTemplates = indexTemplates;
    }

    public List<IndexTemplateMetadata> getIndexTemplates() {
        return indexTemplates;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(indexTemplates);
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
