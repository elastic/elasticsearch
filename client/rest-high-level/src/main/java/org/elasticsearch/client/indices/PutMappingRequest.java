/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Put a mapping definition into one or more indices. If an index already contains mappings,
 * the new mappings will be merged with the existing one. If there are elements that cannot
 * be merged, the request will be rejected.
 */
public class PutMappingRequest extends TimedRequest implements IndicesRequest, ToXContentObject {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, true, true);

    private final String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

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
    public PutMappingRequest source(String mappingSource, XContentType type) {
        this.source = new BytesArray(mappingSource);
        this.xContentType = type;
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
    public PutMappingRequest source(BytesReference bytesReference, XContentType type) {
        this.source = bytesReference;
        this.xContentType = type;
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
