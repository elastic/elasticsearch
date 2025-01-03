/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Puts mapping definition into one or more indices.
 * <p>
 * If the mappings already exists, the new mappings will be merged with the new one. If there are elements
 * that can't be merged are detected, the request will be rejected.
 *
 * @see org.elasticsearch.client.internal.IndicesAdminClient#putMapping(PutMappingRequest)
 * @see AcknowledgedResponse
 */
public class PutMappingRequest extends AcknowledgedRequest<PutMappingRequest> implements IndicesRequest.Replaceable {

    private static final Set<String> RESERVED_FIELDS = Set.of(
        "_uid",
        "_id",
        "_type",
        "_source",
        "_all",
        "_analyzer",
        "_parent",
        "_routing",
        "_index",
        "_size",
        "_timestamp",
        "_ttl",
        "_field_names"
    );

    private String[] indices;

    private IndicesOptions indicesOptions = IndicesOptions.builder()
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
        .wildcardOptions(
            IndicesOptions.WildcardOptions.builder()
                .matchOpen(true)
                .matchClosed(true)
                .includeHidden(false)
                .allowEmptyExpressions(false)
                .resolveAliases(true)
        )
        .gatekeeperOptions(
            IndicesOptions.GatekeeperOptions.builder()
                .allowClosedIndices(true)
                .allowAliasToMultipleIndices(true)
                .ignoreThrottled(false)
                .allowSelectors(false)
        )
        .build();

    private String source;
    private String origin = "";

    private Index concreteIndex;

    private boolean writeIndexOnly;

    public PutMappingRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            String type = in.readOptionalString();
            if (MapperService.SINGLE_MAPPING_NAME.equals(type) == false) {
                throw new IllegalArgumentException("Expected type [_doc] but received [" + type + "]");
            }
        }
        source = in.readString();
        concreteIndex = in.readOptionalWriteable(Index::new);
        origin = in.readOptionalString();
        writeIndexOnly = in.readBoolean();
    }

    public PutMappingRequest() {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
    }

    /**
     * Constructs a new put mapping request against one or more indices. If nothing is set then
     * it will be executed against all indices.
     */
    public PutMappingRequest(String... indices) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (source == null) {
            validationException = addValidationError("mapping source is missing", validationException);
        } else if (source.isEmpty()) {
            validationException = addValidationError("mapping source is empty", validationException);
        }
        if (concreteIndex != null && CollectionUtils.isEmpty(indices) == false) {
            validationException = addValidationError(
                "either concrete index or unresolved indices can be set, concrete index: ["
                    + concreteIndex
                    + "] and indices: "
                    + Arrays.asList(indices),
                validationException
            );
        }
        return validationException;
    }

    /**
     * Sets the indices this put mapping operation will execute on.
     */
    @Override
    public PutMappingRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets a concrete index for this put mapping request.
     */
    public PutMappingRequest setConcreteIndex(Index index) {
        Objects.requireNonNull(index, "index must not be null");
        this.concreteIndex = index;
        return this;
    }

    /**
     * Returns a concrete index for this mapping or <code>null</code> if no concrete index is defined
     */
    public Index getConcreteIndex() {
        return concreteIndex;
    }

    /**
     * The indices the mappings will be put.
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

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * The mapping source definition.
     */
    public String source() {
        return source;
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     *
     * Also supports metadata mapping fields such as `_all` and `_parent` as property definition, these metadata
     * mapping fields will automatically be put on the top level mapping object.
     */
    public PutMappingRequest source(String... source) {
        return source(simpleMapping(source));
    }

    public String origin() {
        return origin;
    }

    public PutMappingRequest origin(String origin) {
        // reserve "null" for bwc.
        this.origin = Objects.requireNonNull(origin);
        return this;
    }

    /**
     * @param source
     *            consisting of field/properties pairs (e.g. "field1",
     *            "type=string,store=true")
     * @throws IllegalArgumentException
     *             if the number of the source arguments is not divisible by two
     * @return the mappings definition
     */
    public static XContentBuilder simpleMapping(String... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("mapping source must be pairs of fieldnames and properties definition.");
        }
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();

            for (int i = 0; i < source.length; i++) {
                String fieldName = source[i++];
                if (RESERVED_FIELDS.contains(fieldName)) {
                    builder.startObject(fieldName);
                    String[] s1 = Strings.splitStringByCommaToArray(source[i]);
                    for (String s : s1) {
                        String[] s2 = Strings.split(s, "=");
                        if (s2.length != 2) {
                            throw new IllegalArgumentException("malformed " + s);
                        }
                        builder.field(s2[0], s2[1]);
                    }
                    builder.endObject();
                }
            }

            builder.startObject("properties");
            for (int i = 0; i < source.length; i++) {
                String fieldName = source[i++];
                if (RESERVED_FIELDS.contains(fieldName)) {
                    continue;
                }

                builder.startObject(fieldName);
                String[] s1 = Strings.splitStringByCommaToArray(source[i]);
                for (String s : s1) {
                    String[] s2 = Strings.split(s, "=");
                    if (s2.length != 2) {
                        throw new IllegalArgumentException("malformed " + s);
                    }
                    builder.field(s2[0], s2[1]);
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return builder;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to generate simplified mapping definition", e);
        }
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequest source(XContentBuilder mappingBuilder) {
        return source(BytesReference.bytes(mappingBuilder), mappingBuilder.contentType());
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequest source(Map<String, ?> mappingSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(mappingSource);
            return source(BytesReference.bytes(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + mappingSource + "]", e);
        }
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequest source(String mappingSource, XContentType xContentType) {
        return source(new BytesArray(mappingSource), xContentType);
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequest source(BytesReference mappingSource, XContentType xContentType) {
        Objects.requireNonNull(xContentType);
        try {
            this.source = XContentHelper.convertToJson(mappingSource, false, false, xContentType);
            return this;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to convert source to json", e);
        }
    }

    public PutMappingRequest writeIndexOnly(boolean writeIndexOnly) {
        this.writeIndexOnly = writeIndexOnly;
        return this;
    }

    public boolean writeIndexOnly() {
        return writeIndexOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_0_0)) {
            out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(source);
        out.writeOptionalWriteable(concreteIndex);
        out.writeOptionalString(origin);
        out.writeBoolean(writeIndexOnly);
    }
}
