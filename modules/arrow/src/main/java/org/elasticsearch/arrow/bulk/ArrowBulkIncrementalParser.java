/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestParser;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.arrow.xcontent.ArrowToString;
import org.elasticsearch.arrow.xcontent.ArrowToXContent;
import org.elasticsearch.arrow.xcontent.XContentBuffer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.libs.arrow.Arrow;
import org.elasticsearch.libs.arrow.ArrowFormatException;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class ArrowBulkIncrementalParser extends BulkRequestParser.XContentIncrementalParser {

    /** XContent format used to encode source documents */
    private static final XContent SOURCE_XCONTENT = XContentType.CBOR.xContent();

    private final DocWriteRequest.OpType defaultOpType;

    private final ArrowIncrementalParser arrowParser;
    private VectorSchemaRoot schemaRoot;
    private Map<Long, Dictionary> dictionaries;

    private Integer idField = null;
    private Integer indexField = null;
    private Integer actionField = null;
    private BitSet valueFields;

    private final ArrowToXContent arrowToXContent = new ArrowToXContent();

    ArrowBulkIncrementalParser(
        DocWriteRequest.OpType defaultOpType,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        @Nullable Boolean defaultRequireDataStream,
        @Nullable Boolean defaultListExecutedPipelines,
        boolean allowExplicitIndex,
        XContentType xContentType,
        XContentParserConfiguration config,
        BiConsumer<IndexRequest, String> indexRequestConsumer,
        Consumer<UpdateRequest> updateRequestConsumer,
        Consumer<DeleteRequest> deleteRequestConsumer
    ) {
        super(
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            defaultRequireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            true, // deprecateOrErrorOnType
            xContentType,
            config,
            indexRequestConsumer,
            updateRequestConsumer,
            deleteRequestConsumer
        );

        this.defaultOpType = defaultOpType;

        // FIXME: hard-coded limit to 100 MiB per record batch. Should we add an AllocationListener that calls ES memory management?
        BufferAllocator allocator = Arrow.newChildAllocator("bulk-ingestion", 0, 100 * 1024 * 1024);

        this.arrowParser = new ArrowIncrementalParser(allocator, new ArrowIncrementalParser.Listener() {
            @Override
            public void startStream(VectorSchemaRoot schemaRoot) throws IOException {
                startArrowStream(schemaRoot);
            }

            @Override
            public void nextBatch(Map<Long, Dictionary> dictionary) throws IOException {
                nextArrowBatch(dictionary);
            }

            @Override
            public void endStream() throws IOException {
                endArrowStream();
            }
        });
    }

    @Override
    public int parse(BytesReference data, boolean lastData) throws IOException {
        return arrowParser.parse(data, lastData);
    }

    @Override
    public void close() {
        super.close();
        if (schemaRoot != null) {
            schemaRoot.close();
            schemaRoot = null;
        }
    }

    private void startArrowStream(VectorSchemaRoot root) {

        this.schemaRoot = root;

        var schemaFields = root.getFieldVectors();
        var valueFields = new BitSet(schemaFields.size());

        for (int i = 0; i < schemaFields.size(); i++) {
            var field = schemaFields.get(i);

            switch (field.getName()) {
                case ArrowBulkAction.ID -> idField = i;
                case ArrowBulkAction.INDEX -> indexField = i;
                case ArrowBulkAction.ACTION -> {
                    var type = field.getMinorType();
                    if (type != Types.MinorType.MAP && type != Types.MinorType.STRUCT) {
                        throw new ArrowFormatException("Field '" + ArrowBulkAction.ACTION + "' should be a map or a struct");
                    }
                    actionField = i;
                }
                // Regular field that will be added to the document.
                default -> valueFields.set(i);
            }
        }

        this.valueFields = valueFields;
    }

    private void nextArrowBatch(Map<Long, Dictionary> dictionary) throws IOException {
        this.dictionaries = dictionary;
        int rowCount = schemaRoot.getRowCount();
        FieldVector idVector = idField == null ? null : schemaRoot.getVector(idField);
        FieldVector indexVector = indexField == null ? null : schemaRoot.getVector(indexField);
        FieldVector actionVector = actionField == null ? null : schemaRoot.getVector(actionField);

        for (int i = 0; i < rowCount; i++) {
            String id = idVector == null ? null : ArrowToString.getString(idVector, i, dictionary);
            String index = indexVector == null ? null : ArrowToString.getString(indexVector, i, dictionary);

            var action = parseAction(actionVector, i, id, index);
            switch (action) {
                case IndexRequest ir -> {
                    ir.source(generateSource(i), SOURCE_XCONTENT.type());
                    indexRequestConsumer.accept(ir, null);
                }
                case UpdateRequest ur -> {
                    // Script updates aren't supported in Arrow format
                    ur.doc(generateSource(i), SOURCE_XCONTENT.type());
                    updateRequestConsumer.accept(ur);
                }
                case DeleteRequest dr -> {
                    deleteRequestConsumer.accept(dr);
                }
                default -> {
                }
            }
        }
    }

    protected BytesReference generateSource(int position) throws IOException {
        var output = new BytesReferenceOutputStream();
        try (var generator = SOURCE_XCONTENT.createGenerator(output)) {
            generator.writeStartObject();
            int rowCount = schemaRoot.getRowCount();
            for (int i = 0; i < rowCount; i++) {
                if (valueFields.get(i)) {
                    arrowToXContent.writeField(schemaRoot.getVector(i), position, dictionaries, generator);
                }
            }
            generator.writeEndObject();
        }

        return output.asBytesReference();
    }

    private void endArrowStream() {
        close();
    }

    // Visible for testing
    DocWriteRequest<?> parseAction(@Nullable FieldVector actionVector, int position, String id, String index) throws IOException {

        DocWriteRequest<?> request;

        try (var generator = new XContentBuffer()) {

            if (actionVector == null) {
                // Create a `{ defaultOpType: {} }` action
                generator.writeStartObject();
                generator.writeFieldName(defaultOpType.getLowercase());
                generator.writeStartObject();
                generator.writeEndObject();
                generator.writeEndObject();
            } else {
                String opType = getNamedString(actionVector, "op_type", position);
                if (opType == null) {
                    opType = defaultOpType.getLowercase();
                }
                // Create a `{ opType: { properties } }` action
                // Note: the "op_type" property may also exist, but the action parser accepts it.
                generator.writeStartObject();
                generator.writeFieldName(opType);
                arrowToXContent.writeValue(actionVector, position, dictionaries, generator);
                generator.writeEndObject();
            }

            request = parseActionLine(generator.asParser());
        }

        if (id != null) {
            if (request.id() != null) {
                throw new ArrowFormatException(
                    "'"
                        + ArrowBulkAction.ID
                        + "' found both as top-level field and in '"
                        + ArrowBulkAction.ACTION
                        + "' at position ["
                        + position
                        + "]"
                );
            }

            switch (request) {
                case IndexRequest ir -> ir.id(id);
                case UpdateRequest ur -> ur.id(id);
                case DeleteRequest ur -> ur.id(id);
                default -> throw new IllegalArgumentException("Unknown request type [" + request.opType() + "]");
            }
        }

        if (index != null) {
            // Testing references on purpose to detect default index passed down to the request
            if (request.index() != defaultIndex) {
                throw new ArrowFormatException(
                    "'"
                        + ArrowBulkAction.INDEX
                        + "' found both as top-level field and in '"
                        + ArrowBulkAction.ACTION
                        + "' at position ["
                        + position
                        + "]"
                );
            }
            request.index(index);
        }

        return request;
    }

    private String getNamedString(FieldVector vector, String name, int position) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        if (vector instanceof MapVector mapVector) {
            // A Map is a variable-size list of structs with two fields, key and value (in this order)
            var data = mapVector.getDataVector();
            var keyVec = (VarCharVector) data.getChildrenFromFields().get(0);
            var valueVec = data.getChildrenFromFields().get(1);

            var key = new Text();
            for (int pos = mapVector.getElementStartIndex(position); pos < mapVector.getElementEndIndex(position); pos++) {
                keyVec.read(pos, key);
                if (Arrays.equals(nameBytes, 0, nameBytes.length, key.getBytes(), 0, (int) key.getLength())) {
                    return ArrowToString.getString(valueVec, pos, this.dictionaries);
                }
            }
            // Not found
            return null;
        }

        if (vector instanceof StructVector structVector) {
            var childVector = structVector.getChild(name);
            return childVector == null ? null : ArrowToString.getString(childVector, position, this.dictionaries);
        }

        for (var child : vector.getChildrenFromFields()) {
            if (child instanceof ValueVector valueVector && valueVector.getName().equals(name)) {
                return ArrowToString.getString(valueVector, position, this.dictionaries);
            }
        }
        return null;
    }
}
