/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ArrowBulkIncrementalParserTests extends ESTestCase {

    // ----- Test Arrow batches and incremental parsing

    public void testBatchingAndChunking() throws IOException {
        checkBatchingAndChunking(1, 10, false);
        checkBatchingAndChunking(1, 10, true);
        checkBatchingAndChunking(2, 10, false);
        checkBatchingAndChunking(2, 10, true);
    }

    /** Create a payload for a 1-column dataframe (int and string), given a number of batches and rows per batch */
    private void checkBatchingAndChunking(int batchCount, int rowCount, boolean incremental) throws IOException {
        byte[] payload;

        // Create a dataframe with two columns: integer and string
        Field intField = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field strField = new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(intField, strField));

        // Create vectors and write them to a byte array
        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var root = VectorSchemaRoot.create(schema, allocator);
        ) {
            var baos = new ByteArrayOutputStream();
            IntVector intVector = (IntVector) root.getVector(0);
            VarCharVector stringVector = (VarCharVector) root.getVector(1);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                for (int batch = 0; batch < batchCount; batch++) {
                    intVector.allocateNew(rowCount);
                    stringVector.allocateNew(rowCount);
                    for (int row = 0; row < rowCount; row++) {
                        int globalRow = row + batch * rowCount;
                        intVector.set(row, globalRow);
                        stringVector.set(row, new Text("row" + globalRow));
                    }
                    root.setRowCount(rowCount);
                    writer.writeBatch();
                }
            }
            payload = baos.toByteArray();
        }

        var operations = new ArrayList<DocWriteRequest<?>>();
        try (var parser = createParser("test", operations)) {
            parse(parser, payload, incremental);
        }
        ;

        assertEquals(batchCount * rowCount, operations.size());

        for (int i = 0; i < operations.size(); i++) {
            IndexRequest operation = (IndexRequest) operations.get(i);

            assertEquals(DocWriteRequest.OpType.INDEX, operation.opType());
            assertEquals("test", operation.index());

            assertEquals(XContentType.CBOR, operation.getContentType());

            var map = operation.sourceAsMap();
            assertEquals(i, map.get("ints"));
            assertEquals("row" + i, map.get("strings"));
        }
    }

    public void testInlineIdAndIndex() throws Exception {
        byte[] payload;

        Field indexField = new Field("_index", FieldType.nullable(new ArrowType.Utf8()), null);
        Field idField = new Field("_id", FieldType.nullable(new ArrowType.Utf8()), null);
        Field intField = new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field strField = new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(List.of(indexField, idField, intField, strField));

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var root = VectorSchemaRoot.create(schema, allocator);
        ) {
            var baos = new ByteArrayOutputStream();
            VarCharVector indexVector = (VarCharVector) root.getVector(0);
            VarCharVector idVector = (VarCharVector) root.getVector(1);
            IntVector intVector = (IntVector) root.getVector(2);
            VarCharVector stringVector = (VarCharVector) root.getVector(3);

            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
                indexVector.allocateNew(4);
                idVector.allocateNew(4);
                intVector.allocateNew(4);
                stringVector.allocateNew(4);

                // No index, no id
                indexVector.setNull(0);
                idVector.setNull(0);
                stringVector.set(0, new Text("row0"));
                intVector.set(0, 0);

                // No index, id
                indexVector.setNull(1);
                idVector.set(1, new Text("id1"));
                stringVector.set(1, new Text("row1"));
                intVector.set(1, 1);

                // Index, no id
                indexVector.set(2, new Text("index2"));
                idVector.setNull(2);
                stringVector.set(2, new Text("row2"));
                intVector.set(2, 2);

                // Index & id
                indexVector.set(3, new Text("index3"));
                idVector.set(3, new Text("id3"));
                stringVector.set(1, new Text("row3"));
                intVector.set(1, 3);

                root.setRowCount(4);
                writer.writeBatch();
            }
            payload = baos.toByteArray();
        }

        var operations = new ArrayList<DocWriteRequest<?>>();
        try (var parser = createParser("defaultIndex", operations)) {
            parse(parser, payload, false);
        }
        ;

        IndexRequest operation = (IndexRequest) operations.get(0);
        assertEquals("defaultIndex", operation.index());
        assertEquals(null, operation.id());

        operation = (IndexRequest) operations.get(1);
        assertEquals("defaultIndex", operation.index());
        assertEquals("id1", operation.id());

        operation = (IndexRequest) operations.get(2);
        assertEquals("index2", operation.index());
        assertEquals(null, operation.id());

        operation = (IndexRequest) operations.get(3);
        assertEquals("index3", operation.index());
        assertEquals("id3", operation.id());

    }

    // ----- Test action decoding

    /** Action as a map of (string, string) */
    public void testActionsAsStringMap() throws Exception {

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var vector = new MapVector("action", allocator, FieldType.nullable(new ArrowType.Map(false)), null);
            var parser = createParser("default-index", List.of())
        ) {
            var w = vector.getWriter();

            w.startMap();

            // Override operation type (default is create)
            w.startEntry();
            w.key().varChar().writeVarChar("op_type");
            w.value().varChar().writeVarChar("update");
            w.endEntry();

            // Override default "default-index" index
            w.startEntry();
            w.key().varChar().writeVarChar("_index");
            w.value().varChar().writeVarChar("first-index");
            w.endEntry();

            // Set if_seq_no as a string, to test a lazy approach with a simple (string, string) map
            w.startEntry();
            w.key().varChar().writeVarChar("if_seq_no");
            w.value().varChar().writeVarChar("3");
            w.endEntry();

            w.endMap();

            w.startMap();

            // Override default "default-index" index
            w.startEntry();
            w.key().varChar().writeVarChar("_index");
            w.value().varChar().writeVarChar("second-index");
            w.endEntry();

            // Override operation type (default is create)
            w.startEntry();
            w.key().varChar().writeVarChar("op_type");
            w.value().varChar().writeVarChar("index");
            w.endEntry();

            // Set version as a string, to test a lazy approach with a simple (string, string) map
            w.startEntry();
            w.key().varChar().writeVarChar("if_seq_no");
            w.value().varChar().writeVarChar("4");
            w.endEntry();

            w.endMap();

            vector.setValueCount(w.getPosition());
            // Value type is varchar
            assertEquals(Types.MinorType.VARCHAR, vector.getChildrenFromFields().get(0).getChildrenFromFields().get(1).getMinorType());

            {
                var request = parser.parseAction(vector, 0, null, null);
                assertEquals(DocWriteRequest.OpType.UPDATE, request.opType());
                assertEquals("first-index", request.index());
                assertEquals(3, request.ifSeqNo());
            }

            {
                var request = parser.parseAction(vector, 1, null, null);
                assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
                assertEquals("second-index", request.index());
                assertEquals(4, request.ifSeqNo());
            }
        }
    }

    /** Action as a map of (string, union(string, int)) */
    public void testActionsAsUnionMap() throws Exception {

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var vector = new MapVector("action", allocator, FieldType.nullable(new ArrowType.Map(false)), null);
            var parser = createParser("default-index", List.of())
        ) {
            var w = vector.getWriter();

            w.startMap();

            // Override operation type (default is create)
            w.startEntry();
            w.key().varChar().writeVarChar("op_type");
            w.value().varChar().writeVarChar("update");
            w.endEntry();

            // Override default "default-index" index
            w.startEntry();
            w.key().varChar().writeVarChar("_index");
            w.value().varChar().writeVarChar("some-index");
            w.endEntry();

            // Set version as a number. This promotes the value field to a union type
            w.startEntry();
            w.key().varChar().writeVarChar("if_seq_no");
            w.value().integer().writeInt(3);
            w.endEntry();

            w.endMap();

            vector.setValueCount(w.getPosition());
            var request = parser.parseAction(vector, 0, null, null);

            // Value type is a union
            assertEquals(Types.MinorType.UNION, vector.getChildrenFromFields().get(0).getChildrenFromFields().get(1).getMinorType());

            assertEquals(DocWriteRequest.OpType.UPDATE, request.opType());
            assertEquals("some-index", request.index());
            assertEquals(3, request.ifSeqNo());
        }
    }

    /** Action as a struct */
    public void testActionsAsStruct() throws Exception {

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            var vector = new StructVector("action", allocator, FieldType.nullable(new ArrowType.Struct()), null);
            var parser = createParser("default-index", List.of())
        ) {
            var w = vector.getWriter();

            w.start();
            w.varChar("op_type").writeVarChar("update");
            w.varChar("_index").writeVarChar("first-index");
            w.integer("if_seq_no").writeInt(3);
            w.end();

            w.start();
            w.varChar("op_type").writeVarChar("index");
            w.varChar("_index").writeVarChar("second-index");
            w.integer("if_seq_no").writeInt(4);
            w.end();

            vector.setValueCount(w.getPosition());

            {
                var request = parser.parseAction(vector, 0, null, null);
                assertEquals(DocWriteRequest.OpType.UPDATE, request.opType());
                assertEquals("first-index", request.index());
                assertEquals(3, request.ifSeqNo());
            }

            {
                var request = parser.parseAction(vector, 1, null, null);
                assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
                assertEquals("second-index", request.index());
                assertEquals(4, request.ifSeqNo());
            }
        }
    }

    // ----- Dictionary encoding
    public void testDictionaryEncoding() throws Exception {

        ByteArrayOutputStream payload = new ByteArrayOutputStream();

        try (
            var allocator = Arrow.rootAllocator().newChildAllocator("test", 0, Long.MAX_VALUE);
            VarCharVector dictVector = new VarCharVector("dict", allocator);
            VarCharVector vector = new VarCharVector("data_field", allocator);
            DictionaryProvider.MapDictionaryProvider dictionaryProvider = new DictionaryProvider.MapDictionaryProvider();
        ) {
            // create dictionary lookup vector
            dictVector.allocateNewSafe();
            dictVector.setSafe(0, "aa".getBytes());
            dictVector.setSafe(1, "bb".getBytes());
            dictVector.setSafe(2, "cc".getBytes());
            dictVector.setValueCount(3);

            // create dictionary
            long dictionaryId = 1L;
            Dictionary dictionary = new Dictionary(dictVector, new DictionaryEncoding(dictionaryId, false, /*indexType=*/null));

            dictionaryProvider.put(dictionary);

            // create original data vector
            vector.allocateNewSafe();
            vector.setSafe(0, "bb".getBytes());
            vector.setSafe(1, "bb".getBytes());
            vector.setSafe(2, "cc".getBytes());
            vector.setSafe(3, "aa".getBytes());
            vector.setValueCount(4);

            // Encode the vector with the dictionary
            IntVector encodedVector = (IntVector) DictionaryEncoder.encode(vector, dictionary);

            // create VectorSchemaRoot
            List<Field> fields = List.of(encodedVector.getField());
            List<FieldVector> vectors = List.of(encodedVector);

            try (
                VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, dictionaryProvider, payload);
            ) {
                // write data
                writer.start();
                writer.writeBatch();
                writer.end();
            }

            var operations = new ArrayList<DocWriteRequest<?>>();
            try (var parser = createParser("defaultIndex", operations)) {
                parse(parser, payload.toByteArray(), false);
            }
            ;

            // Check that dictionary-encoded values were correctly decoded
            assertEquals("bb", ((IndexRequest) operations.get(0)).sourceAsMap().get("data_field"));
            assertEquals("bb", ((IndexRequest) operations.get(1)).sourceAsMap().get("data_field"));
            assertEquals("cc", ((IndexRequest) operations.get(2)).sourceAsMap().get("data_field"));
            assertEquals("aa", ((IndexRequest) operations.get(3)).sourceAsMap().get("data_field"));
        }
    }

    // ----- Utilities

    private static ArrowBulkIncrementalParser createParser(String defaultIndex, List<DocWriteRequest<?>> requests) {

        DocWriteRequest.OpType defaultOpType = DocWriteRequest.OpType.INDEX;
        String defaultRouting = null;
        FetchSourceContext defaultFetchSourceContext = null;
        String defaultPipeline = null;
        Boolean defaultRequireAlias = false;
        Boolean defaultRequireDataStream = false;
        Boolean defaultListExecutedPipelines = false;

        boolean allowExplicitIndex = true;
        XContentType xContentType = null;
        BiConsumer<IndexRequest, String> indexRequestConsumer = (r, t) -> requests.add(r);
        Consumer<UpdateRequest> updateRequestConsumer = requests::add;
        Consumer<DeleteRequest> deleteRequestConsumer = requests::add;

        return new ArrowBulkIncrementalParser(
            defaultOpType,
            defaultIndex,
            defaultRouting,
            defaultFetchSourceContext,
            defaultPipeline,
            defaultRequireAlias,
            defaultRequireDataStream,
            defaultListExecutedPipelines,
            allowExplicitIndex,
            xContentType,
            XContentParserConfiguration.EMPTY.withRestApiVersion(RestApiVersion.current()),
            indexRequestConsumer,
            updateRequestConsumer,
            deleteRequestConsumer
        );
    }

    private void parse(ArrowBulkIncrementalParser parser, byte[] payload, boolean incremental) throws IOException {

        int consumed = 0;
        var request = new BytesArray(payload);

        if (incremental) {
            // Borrowed from BulkRequestParserTests
            for (int i = 0; i < request.length() - 1; ++i) {
                consumed += parser.parse(request.slice(consumed, i - consumed + 1), false);
            }
            consumed += parser.parse(request.slice(consumed, request.length() - consumed), true);
            assertThat(consumed, equalTo(request.length()));
        } else {
            consumed = parser.parse(request, true);
        }

        assertEquals(payload.length, consumed);
    }
}
