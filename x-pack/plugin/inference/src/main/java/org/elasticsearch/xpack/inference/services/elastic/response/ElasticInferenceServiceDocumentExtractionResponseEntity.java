/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.DocumentExtractionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parses the Elastic Inference Service document extraction response, which has the following format:
 * <pre>
 * {
 *   "results": [
 *     {
 *       "content": "# Annual Report 2025\n\n...",
 *       "format": "markdown",
 *       "metadata": {
 *         "title": "Annual Report 2025"
 *       }
 *     }
 *   ],
 *   "usage": {
 *     "total_tokens": 42,
 *     "modalities": {
 *       "document_tokens": 42
 *     }
 *   }
 * }
 * </pre>
 * The {@code format} field is leniently defaulted to markdown when absent, which is the only format the Elastic Inference Service
 * returns for now. The {@code metadata} object contains provider-specific fields and is optional. The {@code usage} object is not
 * parsed.
 */
public class ElasticInferenceServiceDocumentExtractionResponseEntity {

    /**
     * The format of the extracted content returned by the Elastic Inference Service; its Jina Reader provider hands back markdown.
     */
    public static final String MARKDOWN_RESPONSE_FORMAT = "markdown";

    record DocumentExtractionResult(List<DocumentExtractionResultEntry> entries) {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<DocumentExtractionResult, Void> PARSER = new ConstructingObjectParser<>(
            DocumentExtractionResult.class.getSimpleName(),
            true,
            args -> new DocumentExtractionResult((List<DocumentExtractionResultEntry>) args[0])
        );

        static {
            PARSER.declareObjectArray(constructorArg(), DocumentExtractionResultEntry.PARSER::apply, new ParseField("results"));
        }

        record DocumentExtractionResultEntry(String content, String format, Map<String, Object> metadata) {

            @SuppressWarnings("unchecked")
            public static final ConstructingObjectParser<DocumentExtractionResultEntry, Void> PARSER = new ConstructingObjectParser<>(
                DocumentExtractionResultEntry.class.getSimpleName(),
                true,
                args -> new DocumentExtractionResultEntry(
                    (String) args[0],
                    Objects.requireNonNullElse((String) args[1], MARKDOWN_RESPONSE_FORMAT),
                    (Map<String, Object>) args[2]
                )
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField("content"));
                PARSER.declareString(optionalConstructorArg(), new ParseField("format"));
                PARSER.declareField(
                    optionalConstructorArg(),
                    (parser, context) -> parser.mapOrdered(),
                    new ParseField("metadata"),
                    ObjectParser.ValueType.OBJECT
                );
            }

            public DocumentExtractionResults.Result toResult() {
                return new DocumentExtractionResults.Result(content, format, metadata);
            }
        }
    }

    public static InferenceServiceResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var documentExtractionResult = DocumentExtractionResult.PARSER.apply(jsonParser, null);

            return new DocumentExtractionResults(
                documentExtractionResult.entries.stream().map(DocumentExtractionResult.DocumentExtractionResultEntry::toResult).toList()
            );
        }
    }

    private ElasticInferenceServiceDocumentExtractionResponseEntity() {}
}
