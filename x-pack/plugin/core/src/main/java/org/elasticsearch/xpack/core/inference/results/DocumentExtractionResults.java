/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Writes a document extraction result in the following json format:
 * <pre>
 * {
 *   "document_extraction": [
 *     {
 *       "content": "# Annual Report 2025\n\n...",
 *       "format": "markdown",
 *       "metadata": {
 *         "title": "Annual Report 2025"
 *       }
 *     }
 *   ]
 * }
 * </pre>
 * The {@code content} and {@code format} fields are common across providers, while the {@code metadata} object contains
 * provider-specific fields and is omitted when a provider does not return any metadata.
 */
public record DocumentExtractionResults(List<Result> results) implements InferenceServiceResults {

    public static final String NAME = "document_extraction_service_results";
    public static final String DOCUMENT_EXTRACTION = "document_extraction";

    public DocumentExtractionResults(StreamInput in) throws IOException {
        this(in.readCollectionAsList(Result::new));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(DOCUMENT_EXTRACTION, results.iterator());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(results);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return results;
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(DOCUMENT_EXTRACTION, results.stream().map(Result::asMap).collect(Collectors.toList()));

        return map;
    }

    /**
     * The extracted content of a single input document.
     *
     * @param content  The extracted document content
     * @param format   The format of the extracted content, e.g. {@code markdown}
     * @param metadata Provider-specific metadata about the extracted document, or an empty map if the provider did not return any
     */
    public record Result(String content, String format, Map<String, Object> metadata) implements InferenceResults, Writeable {

        public static final String CONTENT = "content";
        public static final String FORMAT = "format";
        public static final String METADATA = "metadata";

        public Result(String content, String format, @Nullable Map<String, Object> metadata) {
            this.content = Objects.requireNonNull(content);
            this.format = Objects.requireNonNull(format);
            this.metadata = Objects.requireNonNullElse(metadata, Map.of());
        }

        public Result(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readGenericMap());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(content);
            out.writeString(format);
            out.writeGenericMap(metadata);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONTENT, content);
            builder.field(FORMAT, format);
            if (metadata.isEmpty() == false) {
                builder.field(METADATA, metadata);
            }
            builder.endObject();

            return builder;
        }

        @Override
        public String getResultsField() {
            return CONTENT;
        }

        @Override
        public Map<String, Object> asMap() {
            return asMap(CONTENT);
        }

        @Override
        public Map<String, Object> asMap(String outputField) {
            Map<String, Object> map = new LinkedHashMap<>();
            map.put(outputField, content);
            map.put(FORMAT, format);
            if (metadata.isEmpty() == false) {
                map.put(METADATA, metadata);
            }
            return map;
        }

        @Override
        public Object predictedValue() {
            return content;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
