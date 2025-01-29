/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.Metadata;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

final class WriteableIngestDocument implements Writeable, ToXContentFragment {

    static final String SOURCE_FIELD = "_source";
    static final String INGEST_FIELD = "_ingest";
    static final String DOC_FIELD = "doc";
    private final IngestDocument ingestDocument;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<WriteableIngestDocument, Void> INGEST_DOC_PARSER = new ConstructingObjectParser<>(
        "ingest_document",
        true,
        a -> {
            Map<String, Object> source = (Map<String, Object>) a[5];
            Map<String, Object> sourceAndMetadata = Maps.newHashMapWithExpectedSize(5 + source.size());
            sourceAndMetadata.putAll(source);
            sourceAndMetadata.put(Metadata.INDEX.getFieldName(), a[0]);
            sourceAndMetadata.put(Metadata.ID.getFieldName(), a[1]);
            if (a[2] != null) {
                sourceAndMetadata.put(Metadata.ROUTING.getFieldName(), a[2]);
            }
            if (a[3] != null) {
                sourceAndMetadata.put(Metadata.VERSION.getFieldName(), a[3]);
            }
            if (a[4] != null) {
                sourceAndMetadata.put(Metadata.VERSION_TYPE.getFieldName(), a[4]);
            }
            Map<String, Object> ingestMetadata = (Map<String, Object>) a[6];
            return new WriteableIngestDocument(sourceAndMetadata, ingestMetadata);
        }
    );
    static {
        INGEST_DOC_PARSER.declareString(constructorArg(), new ParseField(Metadata.INDEX.getFieldName()));
        INGEST_DOC_PARSER.declareString(constructorArg(), new ParseField(Metadata.ID.getFieldName()));
        INGEST_DOC_PARSER.declareString(optionalConstructorArg(), new ParseField(Metadata.ROUTING.getFieldName()));
        INGEST_DOC_PARSER.declareLong(optionalConstructorArg(), new ParseField(Metadata.VERSION.getFieldName()));
        INGEST_DOC_PARSER.declareString(optionalConstructorArg(), new ParseField(Metadata.VERSION_TYPE.getFieldName()));
        INGEST_DOC_PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField(SOURCE_FIELD));
        INGEST_DOC_PARSER.declareObject(constructorArg(), (p, c) -> {
            Map<String, Object> ingestMap = p.map();
            ingestMap.computeIfPresent("timestamp", (k, o) -> ZonedDateTime.parse((String) o));
            return ingestMap;
        }, new ParseField(INGEST_FIELD));
    }

    public static final ConstructingObjectParser<WriteableIngestDocument, Void> PARSER = new ConstructingObjectParser<>(
        "writeable_ingest_document",
        true,
        a -> (WriteableIngestDocument) a[0]
    );
    static {
        PARSER.declareObject(constructorArg(), INGEST_DOC_PARSER, new ParseField(DOC_FIELD));
    }

    /**
     * Builds a writeable ingest document that wraps a copy of the passed-in, non-null ingest document.
     *
     * @throws IllegalArgumentException if the passed-in ingest document references itself
     */
    WriteableIngestDocument(IngestDocument ingestDocument) {
        assert ingestDocument != null;
        this.ingestDocument = new IngestDocument(ingestDocument); // internal defensive copy
    }

    /**
     * Builds a writeable ingest document by constructing the wrapped ingest document from the passed-in maps.
     * <p>
     * This is intended for cases like deserialization, where we know the passed-in maps aren't self-referencing,
     * and where a defensive copy is unnecessary.
     */
    private WriteableIngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        this.ingestDocument = new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    WriteableIngestDocument(StreamInput in) throws IOException {
        this(in.readGenericMap(), in.readGenericMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(ingestDocument.getSourceAndMetadata());
        out.writeGenericMap(ingestDocument.getIngestMetadata());
    }

    IngestDocument getIngestDocument() {
        return ingestDocument;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(DOC_FIELD);
        org.elasticsearch.script.Metadata metadata = ingestDocument.getMetadata();
        for (String key : metadata.keySet()) {
            Object value = metadata.get(key);
            if (value != null) {
                builder.field(key, value.toString());
            }
        }
        builder.field(SOURCE_FIELD, ingestDocument.getSource());
        builder.field(INGEST_FIELD, ingestDocument.getIngestMetadata());
        builder.endObject();
        return builder;
    }

    public static WriteableIngestDocument fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return ingestDocument.toString();
    }
}
