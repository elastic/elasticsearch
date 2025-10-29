/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

public class RerankServiceSettings extends FilteredXContentObject implements VersionedNamedWriteable {
    private final String NAME = "rerank_service_settings";

    public static final String LONG_DOCUMENT_STRATEGY = "long_document_strategy";
    public static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    private final LongDocumentStrategy longDocumentStrategy;
    private final Integer maxChunksPerDoc;

    public static RerankServiceSettings fromMap(Map<String, Object> map) {
        var validationException = new ValidationException();

        LongDocumentStrategy longDocumentStrategy = extractOptionalEnum(
            map,
            LONG_DOCUMENT_STRATEGY,
            ModelConfigurations.SERVICE_SETTINGS,
            LongDocumentStrategy::fromString,
            EnumSet.allOf(LongDocumentStrategy.class),
            validationException
        );

        Integer maxChunksPerDoc = extractOptionalPositiveInteger(
            map,
            MAX_CHUNKS_PER_DOC,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        if ((longDocumentStrategy == null || LongDocumentStrategy.TRUNCATE.equals(longDocumentStrategy)) && maxChunksPerDoc != null) {
            validationException.addValidationError(
                "Setting [" + MAX_CHUNKS_PER_DOC + "] cannot be set without setting [" + LONG_DOCUMENT_STRATEGY + "]"
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new RerankServiceSettings(longDocumentStrategy, maxChunksPerDoc);
    }

    public RerankServiceSettings(LongDocumentStrategy longDocumentStrategy, Integer maxChunksPerDoc) {
        this.longDocumentStrategy = longDocumentStrategy;
        this.maxChunksPerDoc = maxChunksPerDoc;
    }

    public RerankServiceSettings(RerankServiceSettings other) {
        this.longDocumentStrategy = other.longDocumentStrategy;
        this.maxChunksPerDoc = other.maxChunksPerDoc;
    }

    public RerankServiceSettings(StreamInput in) throws IOException {
        longDocumentStrategy = in.readOptionalEnum(LongDocumentStrategy.class);
        maxChunksPerDoc = in.readOptionalInt();
        // TODO: Add transport version
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(longDocumentStrategy);
        out.writeOptionalInt(maxChunksPerDoc);
        // TODO: Add transport version
    }

    public LongDocumentStrategy getLongDocumentStrategy() {
        return longDocumentStrategy;
    }

    public Integer getMaxChunksPerDoc() {
        return maxChunksPerDoc;
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (longDocumentStrategy != null) {
            builder.field(LONG_DOCUMENT_STRATEGY, longDocumentStrategy.strategyName);
        }
        if (maxChunksPerDoc != null) {
            builder.field(MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RerankServiceSettings that = (RerankServiceSettings) o;
        return longDocumentStrategy == that.longDocumentStrategy && java.util.Objects.equals(maxChunksPerDoc, that.maxChunksPerDoc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longDocumentStrategy, maxChunksPerDoc);
    }
}
