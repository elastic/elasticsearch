/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.xcontent;

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
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.libs.arrow.ArrowFormatException;

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

        Void x = switch (vector.getMinorType()) {
            //---- Numbers

            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> {
                generator.writeNumber(((BaseIntVector) vector).getValueAsLong(position));
                yield null;
            }

            case FLOAT2, FLOAT4, FLOAT8 -> {
                generator.writeNumber(((FloatingPointVector) vector).getValueAsDouble(position));
                yield null;
            }

            case DECIMAL -> {
                var dVector = (DecimalVector) vector;
                generator.writeNumber(dVector.getObjectNotNull(position));
                yield null;
            }

            case DECIMAL256 -> {
                var dVector = (Decimal256Vector) vector;
                generator.writeNumber(dVector.getObjectNotNull(position));
                yield null;
            }

            //---- Booleans

            case BIT -> {
                generator.writeBoolean(((BitVector) vector).get(position) != 0);
                yield null;
            }

            //---- Strings

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                // TODO: maybe we can avoid a copy using bytesVector.getDatapointer()?
                generator.writeString(new String(bytesVector.get(position), StandardCharsets.UTF_8));
                yield null;
            }

            //---- Binary

            case VARBINARY, LARGEVARBINARY, VIEWVARBINARY -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                generator.writeBinary(bytesVector.get(position));
                yield null;
            }

            case FIXEDSIZEBINARY -> {
                var bytesVector = (FixedSizeBinaryVector) vector;
                generator.writeBinary(bytesVector.get(position));
                yield null;
            }

            //----- Timestamps
            //
            // Timestamp values are relative to the Unix epoch in UTC, with an optional timezone.
            // The ES date type has no timezone, so we drop this information.
            // (TODO: define where the TZ should go, e.g. providing the name of a TZ field in the field's metadata)
            //
            // Seconds and millis are stored as millis, and micros and nanos as nanos, so that there's
            // no precision loss. (FIXME: define this conversion using the ES field type)

            case TIMESTAMPSEC, TIMESTAMPMICRO, TIMESTAMPSECTZ, TIMESTAMPMICROTZ -> {
                var tsVector = (TimeStampVector) vector;
                generator.writeNumber(tsVector.get(position) * 1000L);
                yield null;
            }

            case TIMESTAMPMILLI, TIMESTAMPNANO, TIMESTAMPMILLITZ, TIMESTAMPNANOTZ -> {
                var tsVector = (TimeStampVector) vector;
                generator.writeNumber(tsVector.get(position));
                yield null;
            }

            //---- Date
            //
            // Time since the epoch, in days or millis evenly divisible 86_400_000
            // Stored as millis

            case DATEDAY -> {
                var ddVector = (DateDayVector) vector;
                generator.writeNumber(ddVector.get(position) * 1000);
                yield null;
            }

            case DATEMILLI -> {
                var dmVector = (DateMilliVector) vector;
                generator.writeNumber(dmVector.get(position));
                yield null;
            }

            //----- Time
            //
            // Time since midnight, either a 32-bit or 64-bit signed integer.
            // There is no equivalent in ES, but we still convert to millis or nanos
            // to be consistent with timestamps.

            case TIMESEC -> {
                var tVector = (TimeSecVector) vector;
                generator.writeNumber(tVector.get(position) * 1000);
                yield null;
            }

            case TIMEMILLI -> {
                var tVector = (TimeMilliVector) vector;
                generator.writeNumber(tVector.get(position));
                yield null;
            }

            case TIMEMICRO -> {
                var tVector = (TimeMicroVector) vector;
                generator.writeNumber(tVector.get(position) * 1000);
                yield null;
            }

            case TIMENANO -> {
                var tsVector = (TimeNanoVector) vector;
                generator.writeNumber(tsVector.get(position));
                yield null;
            }

            //---- Other fixed size types

            case DURATION -> {
                var dVector = (DurationVector) vector;
                long value = DurationVector.get(dVector.getDataBuffer(), position);

                value *= switch (dVector.getUnit()) {
                    case SECOND, MICROSECOND -> 1000L;
                    case MILLISECOND, NANOSECOND -> 1L;
                };

                generator.writeNumber(value);
                yield null;
            }

            //---- Structured types

            case LIST, FIXED_SIZE_LIST, LISTVIEW -> {
                var listVector = (BaseListVector) vector;
                var valueVector = listVector.getChildrenFromFields().getFirst();
                int start = listVector.getElementStartIndex(position);
                int end = listVector.getElementEndIndex(position);

                generator.writeStartArray();
                for (int i = start; i < end; i++) {
                    writeValue(valueVector, i, generator);
                }
                generator.writeEndArray();
                yield null;
            }

            case MAP -> {
                // A map is a container vector that is composed of a list of struct values with "key" and "value" fields. The MapVector
                // is nullable, but if a map is set at a given index, there must be an entry. In other words, the StructVector data is
                // non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can be null.

                var mapVector = (MapVector) vector;
                var structVector = (StructVector) mapVector.getChildrenFromFields().getFirst();
                var kVector = structVector.getChildrenFromFields().getFirst();
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
                yield null;
            }

            case STRUCT -> {
                var structVector = (StructVector) vector;
                generator.writeStartObject();
                for (var field : structVector.getChildrenFromFields()) {
                    generator.writeFieldName(field.getName());
                    writeValue(field, position, generator);
                }
                generator.writeEndObject();
                yield null;
            }

            case DENSEUNION -> {
                var unionVector = (DenseUnionVector) vector;
                var typeId = unionVector.getTypeId(position);
                var valueVector = unionVector.getVectorByType(typeId);
                var valuePosition = unionVector.getOffset(position);

                writeValue(valueVector, valuePosition, generator);
                yield null;
            }

            case UNION -> { // sparse union
                var unionVector = (UnionVector) vector;
                var typeId = unionVector.getTypeValue(position);
                var valueVector = unionVector.getVectorByType(typeId);

                writeValue(valueVector, position, generator);
                yield null;
            }

            case NULL -> {
                // Should  have been handled at the beginning of this method,
                // but keep it to have exhaustive coverage of enum values.
                generator.writeNull();
                yield null;
            }


            case INTERVALYEAR, INTERVALDAY, INTERVALMONTHDAYNANO, // ES doesn't have any interval types
                 LARGELIST, LARGELISTVIEW // 64-bit vector support is incomplete
                -> throw new JsonParseException(
                "Arrow type [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
            );

            case RUNENDENCODED -> {
                var reVector = (RunEndEncodedVector) vector;
                // Caveat: performance could be improved. getRunEnd() does a binary search for the position
                // in the value array, and so does isNull() at the top of this method. If run-end encoding
                // is heavily used, we could use an optimized cursor structure that is moved forward at
                // each iteration in the calling loop.
                writeValue(reVector.getValuesVector(), reVector.getRunEnd(position), generator);
                yield null;
            }

            case EXTENSIONTYPE -> {
                writeExtensionValue(vector, position, generator);
                yield null;
            }
        };
    }

    private void writeExtensionValue(ValueVector vector, int position, JsonGenerator generator) throws IOException {

    }
}
