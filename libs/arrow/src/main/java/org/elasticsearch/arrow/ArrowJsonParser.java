/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.JsonParserDelegate;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

/**
 * A Jackson parser that reads Arrow record batches. The JSON result is an array
 * containing one object per line.
 * <p>
 * To reduce memory allocations, each line is read in a Jackson TokenBuffer, created
 * lazily as the parser consumer fetched events. Similarly, record batches are read
 * lazily.
 */
public class ArrowJsonParser extends JsonParserDelegate {

    private static final EnumSet<Types.MinorType> STRING_TYPES = EnumSet.of(
        Types.MinorType.VARCHAR,
        Types.MinorType.LARGEVARCHAR,
        Types.MinorType.VIEWVARCHAR
    );

    private final ArrowStreamReader reader;
    private boolean done = false;

    private int rootPosition = 0;

    public ArrowJsonParser(InputStream in) throws IOException {
        this(in, new RootAllocator());
    }

    public ArrowJsonParser(InputStream in, BufferAllocator allocator) throws IOException {
        super(null);
        this.reader = new ArrowStreamReader(in, allocator);
        // Read schema early
        reader.getVectorSchemaRoot();

        var p = new JsonFactory().createParser("");
        var tokens = new TokenBuffer(p);
        tokens.writeStartArray();
        delegate = tokens.asParser();
    }

    @Override
    public JsonToken nextToken() throws IOException {
        var result = delegate.nextToken();
        if (result == null && done == false) {
            fillBuffer();
            return nextToken();
        } else {
            return result;
        }
    }

    private void fillBuffer() throws IOException {
        if (done) {
            return;
        }

        var buffer = new TokenBuffer(this);

        if (rootPosition == 0) {
            if (this.reader.loadNextBatch() == false) {
                // End of stream
                buffer.writeEndArray();
                delegate = buffer.asParser();
                done = true;
                return;
            }
        }

        var schema = reader.getVectorSchemaRoot();

        buffer.writeStartObject();
        for (var vector : schema.getFieldVectors()) {
            buffer.writeFieldName(vector.getName());
            writeValue(vector, rootPosition, buffer);
        }
        buffer.writeEndObject();

        rootPosition++;
        if (rootPosition >= schema.getRowCount()) {
            // Read a new batch at the next iteration
            rootPosition = 0;
        }
        delegate = buffer.asParser();
    }

    private void writeValue(ValueVector vector, int position, JsonGenerator generator) throws IOException {

        if (vector.isNull(position)) {
            generator.writeNull();
            return;
        }

        switch (vector.getMinorType()) {
            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> {
                generator.writeNumber(((BaseIntVector) vector).getValueAsLong(position));
            }

            case FLOAT2, FLOAT4, FLOAT8 -> {
                generator.writeNumber(((FloatingPointVector) vector).getValueAsDouble(position));
            }

            case BIT -> {
                generator.writeBoolean(((BitVector) vector).get(position) != 0);
            }

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                // TODO: maybe we can avoid a copy using bytesVector.getDatapointer()?
                generator.writeString(new String(bytesVector.get(position), StandardCharsets.UTF_8));
            }

            case VARBINARY, LARGEVARBINARY, VIEWVARBINARY -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                generator.writeBinary(bytesVector.get(position));
            }

            case LIST, FIXED_SIZE_LIST, LISTVIEW -> {
                var listVector = (BaseListVector) vector;
                var valueVector = listVector.getChildrenFromFields().get(0);
                int start = listVector.getElementStartIndex(position);
                int end = listVector.getElementEndIndex(position);

                generator.writeStartArray();
                for (int i = start; i < end; i++) {
                    writeValue(valueVector, i, generator);
                }
                generator.writeEndArray();
            }

            case TIMESTAMPMILLI -> {
                var tsVector = (TimeStampVector) vector;
                generator.writeNumber(tsVector.get(position));
            }

            case TIMEMICRO -> {
                var tsVector = (TimeStampVector) vector;
                // FIXME: format as string with enough decimal positions
                generator.writeNumber(tsVector.get(position) / 1000);
            }

            case TIMENANO -> {
                var tsVector = (TimeStampVector) vector;
                // FIXME: format as string with enough decimal positions
                generator.writeNumber(tsVector.get(position) / 1_000_000);
            }

            case MAP -> {
                // A map is a container vector that is composed of a list of struct values with "key" and "value" fields. The MapVector
                // is nullable, but if a map is set at a given index, there must be an entry. In other words, the StructVector data is
                // non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can be null.

                var mapVector = (MapVector) vector;
                var structVector = (StructVector) mapVector.getChildrenFromFields().get(0);
                var kVector = structVector.getChildrenFromFields().get(0);
                if (STRING_TYPES.contains(kVector.getMinorType()) == false) {
                    throw new ArrowFormatException("Maps must have string keys");
                }

                var keyVector = (VarBinaryVector) kVector;
                var valueVector = structVector.getChildrenFromFields().get(1);

                int start = mapVector.getElementStartIndex(position);
                int end = mapVector.getElementEndIndex(position);

                generator.writeStartObject();
                for (int i = start; i < end; i++) {
                    var key = new String(keyVector.get(i), StandardCharsets.UTF_8);
                    generator.writeFieldName(key);
                    writeValue(valueVector, i, generator);
                }
                generator.writeEndObject();
            }

            case STRUCT -> {
                var structVector = (StructVector) vector;
                generator.writeStartObject();
                for (var field : structVector.getChildrenFromFields()) {
                    generator.writeFieldName(field.getName());
                    writeValue(field, position, generator);
                }
                generator.writeEndObject();
            }

            case DENSEUNION -> {
                var unionVector = (DenseUnionVector) vector;
                var typeId = unionVector.getTypeId(position);
                var valueVector = unionVector.getVectorByType(typeId);
                var valuePosition = unionVector.getOffset(position);

                writeValue(valueVector, valuePosition, generator);
            }

            case UNION -> { // sparse union
                var unionVector = (UnionVector) vector;
                var typeId = unionVector.getTypeValue(position);
                var valueVector = unionVector.getVectorByType(typeId);

                writeValue(valueVector, position, generator);
            }

            default -> throw new JsonParseException(
                "Arrow type [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
            );
        }
    }
}
