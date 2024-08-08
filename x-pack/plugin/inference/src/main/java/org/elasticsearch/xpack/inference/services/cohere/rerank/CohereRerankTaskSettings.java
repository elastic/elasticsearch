/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalBoolean;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;

/**
 * Defines the task settings for the cohere rerank service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/rerank-1">See api docs for details.</a>
 * </p>
 */
public class CohereRerankTaskSettings implements TaskSettings {

    public static final String NAME = "cohere_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";
    public static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    static final CohereRerankTaskSettings EMPTY_SETTINGS = new CohereRerankTaskSettings(null, null, null);

    public static CohereRerankTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        Boolean returnDocuments = extractOptionalBoolean(map, RETURN_DOCUMENTS, validationException);
        Integer topNDocumentsOnly = extractOptionalPositiveInteger(
            map,
            TOP_N_DOCS_ONLY,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );
        Integer maxChunksPerDoc = extractOptionalPositiveInteger(
            map,
            MAX_CHUNKS_PER_DOC,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return of(topNDocumentsOnly, returnDocuments, maxChunksPerDoc);
    }

    /**
     * Creates a new {@link CohereRerankTaskSettings} by preferring non-null fields from the request settings over the original settings.
     *
     * @param originalSettings    the settings stored as part of the inference entity configuration
     * @param requestTaskSettings the settings passed in within the task_settings field of the request
     * @return a constructed {@link CohereRerankTaskSettings}
     */
    public static CohereRerankTaskSettings of(CohereRerankTaskSettings originalSettings, CohereRerankTaskSettings requestTaskSettings) {
        return new CohereRerankTaskSettings(
            requestTaskSettings.getTopNDocumentsOnly() != null
                ? requestTaskSettings.getTopNDocumentsOnly()
                : originalSettings.getTopNDocumentsOnly(),
            requestTaskSettings.getReturnDocuments() != null
                ? requestTaskSettings.getReturnDocuments()
                : originalSettings.getReturnDocuments(),
            requestTaskSettings.getMaxChunksPerDoc() != null
                ? requestTaskSettings.getMaxChunksPerDoc()
                : originalSettings.getMaxChunksPerDoc()
        );
    }

    public static CohereRerankTaskSettings of(Integer topNDocumentsOnly, Boolean returnDocuments, Integer maxChunksPerDoc) {
        return new CohereRerankTaskSettings(topNDocumentsOnly, returnDocuments, maxChunksPerDoc);
    }

    private final Integer topNDocumentsOnly;
    private final Boolean returnDocuments;
    private final Integer maxChunksPerDoc;

    public CohereRerankTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalInt(), in.readOptionalBoolean(), in.readOptionalInt());
    }

    public CohereRerankTaskSettings(
        @Nullable Integer topNDocumentsOnly,
        @Nullable Boolean doReturnDocuments,
        @Nullable Integer maxChunksPerDoc
    ) {
        this.topNDocumentsOnly = topNDocumentsOnly;
        this.returnDocuments = doReturnDocuments;
        this.maxChunksPerDoc = maxChunksPerDoc;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (topNDocumentsOnly != null) {
            builder.field(TOP_N_DOCS_ONLY, topNDocumentsOnly);
        }
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS, returnDocuments);
        }
        if (maxChunksPerDoc != null) {
            builder.field(MAX_CHUNKS_PER_DOC, maxChunksPerDoc);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalInt(topNDocumentsOnly);
        out.writeOptionalBoolean(returnDocuments);
        out.writeOptionalInt(maxChunksPerDoc);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CohereRerankTaskSettings that = (CohereRerankTaskSettings) o;
        return Objects.equals(returnDocuments, that.returnDocuments)
            && Objects.equals(topNDocumentsOnly, that.topNDocumentsOnly)
            && Objects.equals(maxChunksPerDoc, that.maxChunksPerDoc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(returnDocuments, topNDocumentsOnly, maxChunksPerDoc);
    }

    public static String invalidInputTypeMessage(InputType inputType) {
        return Strings.format("received invalid input type value [%s]", inputType.toString());
    }

    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Integer getMaxChunksPerDoc() {
        return maxChunksPerDoc;
    }

}
