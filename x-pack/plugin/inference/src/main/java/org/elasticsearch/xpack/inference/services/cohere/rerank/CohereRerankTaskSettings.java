/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Defines the task settings for the cohere rerank service.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/rerank-1">See api docs for details.</a>
 * </p>
 */
public class CohereRerankTaskSettings implements TaskSettings, TopNProvider {

    public static final String NAME = "cohere_rerank_task_settings";
    public static final String RETURN_DOCUMENTS = "return_documents";
    public static final String TOP_N_DOCS_ONLY = "top_n";
    public static final String MAX_CHUNKS_PER_DOC = "max_chunks_per_doc";

    static final CohereRerankTaskSettings EMPTY_SETTINGS = new CohereRerankTaskSettings(null, null, null);

    private static final ConstructingObjectParser<CohereRerankTaskSettings, Void> REQUEST_PARSER = createParser(false);
    private static final ConstructingObjectParser<CohereRerankTaskSettings, Void> PERSISTENT_PARSER = createParser(true);

    static ConstructingObjectParser<CohereRerankTaskSettings, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CohereRerankTaskSettings, Void> parser = new ConstructingObjectParser<>(
            ModelConfigurations.TASK_SETTINGS,
            ignoreUnknownFields,
            args -> new CohereRerankTaskSettings((Integer) args[0], (Boolean) args[1], (Integer) args[2])
        );
        parser.declareField(
            optionalConstructorArg(),
            (p, ctx) -> ObjectParserUtils.parsePositiveInteger(p, TOP_N_DOCS_ONLY),
            new ParseField(TOP_N_DOCS_ONLY),
            ObjectParser.ValueType.INT
        );
        parser.declareBoolean(optionalConstructorArg(), new ParseField(RETURN_DOCUMENTS));
        parser.declareField(
            optionalConstructorArg(),
            (p, ctx) -> ObjectParserUtils.parsePositiveInteger(p, MAX_CHUNKS_PER_DOC),
            new ParseField(MAX_CHUNKS_PER_DOC),
            ObjectParser.ValueType.INT
        );
        return parser;
    }

    public static CohereRerankTaskSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
        }
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
    public boolean isEmpty() {
        return topNDocumentsOnly == null && returnDocuments == null && maxChunksPerDoc == null;
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
        return TransportVersion.minimumCompatible();
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

    public Boolean getDoesReturnDocuments() {
        return returnDocuments;
    }

    public Integer getTopNDocumentsOnly() {
        return topNDocumentsOnly;
    }

    @Override
    public Integer getTopN() {
        return getTopNDocumentsOnly();
    }

    public Boolean getReturnDocuments() {
        return returnDocuments;
    }

    public Integer getMaxChunksPerDoc() {
        return maxChunksPerDoc;
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        CohereRerankTaskSettings updatedSettings = CohereRerankTaskSettings.fromMap(newSettings, ConfigurationParseContext.REQUEST);
        return CohereRerankTaskSettings.of(this, updatedSettings);
    }
}
