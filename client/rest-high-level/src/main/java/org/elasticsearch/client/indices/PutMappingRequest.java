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

package org.elasticsearch.client.indices;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Put a mapping definition into one or more indices. If an index already contains mappings,
 * the new mappings will be merged with the existing one. If there are elements that cannot
 * be merged, the request will be rejected.
 */
public class PutMappingRequest extends TimedRequest implements IndicesRequest, ToXContentObject {

    private final String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, true);

    private BytesReference source;
    private XContentType xContentType;

    /**
     * Constructs a new put mapping request against one or more indices. If no indices
     * are provided then it will be executed against all indices.
     */
    public PutMappingRequest(String... indices) {
        this.indices = indices;
    }

    /**
     * The indices into which the mappings will be put.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public PutMappingRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * The mapping source definition.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * The {@link XContentType} of the mapping source.
     */
    public XContentType xContentType() {
        return xContentType;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(Map<String, ?> mappingSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(mappingSource);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + mappingSource + "]", e);
        }
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(String mappingSource, XContentType xContentType) {
        this.source = new BytesArray(mappingSource);
        this.xContentType = xContentType;
        return this;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(XContentBuilder builder) {
        this.source = BytesReference.bytes(builder);
        this.xContentType = builder.contentType();
        return this;
    }

    /**
     * The mapping source definition.
     *
     * Note that the definition should *not* be nested under a type name.
     */
    public PutMappingRequest source(BytesReference source, XContentType xContentType) {
        this.source = source;
        this.xContentType = xContentType;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (source != null) {
            try (InputStream stream = source.streamInput()) {
                builder.rawValue(stream, xContentType);
            }
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
