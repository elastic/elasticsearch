/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.xcontent;

import com.fasterxml.jackson.core.JsonParseException;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.ReusableByteArray;
import org.elasticsearch.libs.arrow.ArrowFormatException;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;

/**
 * Utility methods to serialize Arrow dataframes to XContent events.
 * <p>
 * Limitations and caveats:
 * <ul>
 * <li>time and timestamps are converted to milliseconds or nanoseconds depending on their unit
 * </li>
 * <li>some types aren't implemented
 * </li>
 * </ul>
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#data-types">Arrow data types</a>
 * @see <a href="https://github.com/apache/arrow/blob/main/format/Schema.fbs">Arrow schema</a>
 */
public class ArrowToXContent {

    private static final EnumSet<Types.MinorType> STRING_TYPES = EnumSet.of(
        Types.MinorType.VARCHAR,
        Types.MinorType.LARGEVARCHAR,
        Types.MinorType.VIEWVARCHAR
    );

    // Reusable buffer to transfer strings and byte values of length smaller than MAX_BUFFER_SIZE
    private final ReusableByteArray bytesBuffer = new ReusableByteArray();
    private static final int MAX_BUFFER_SIZE = 1024*1024;

    private ReusableByteArray getBuffer(int length) {
        return length > MAX_BUFFER_SIZE ? new ReusableByteArray() : bytesBuffer;
    }

    /**
     * Write a field and its value from an Arrow vector as XContent
     *
     * @param vector the Arrow vector
     * @param position the value position in the vector
     * @param dictionaries to look up values for dictionary-encoded vectors
     * @param generator XContent output
     */
    public void writeField(ValueVector vector, int position, Map<Long, Dictionary> dictionaries, XContentGenerator generator)
        throws IOException {
        generator.writeFieldName(vector.getName());
        writeValue(vector, position, dictionaries, generator);
    }

    /**
     * Write a value from an Arrow vector as XContent
     *
     * @param vector the Arrow vector
     * @param position the value position in the vector
     * @param dictionaries to look up values for dictionary-encoded vectors
     * @param generator XContent output
     */
    public void writeValue(ValueVector vector, int position, Map<Long, Dictionary> dictionaries, XContentGenerator generator)
        throws IOException {

        if (vector.isNull(position)) {
            generator.writeNull();
            return;
        }

        var field = vector.getField();
        var extension = field.getMetadata().get(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME);

        var dictEncoding = field.getDictionary();
        if (dictEncoding != null) {
            // Note: to improve performance and reduce GC thrashing, we could eagerly convert dictionary
            // VarCharVectors to String arrays (likely the most frequent use of dictionaries)
            Dictionary dictionary = dictionaries.get(dictEncoding.getId());
            // The spec allows any integer type, although signed 32 bits are recommended
            position = (int) ((BaseIntVector) vector).getValueAsLong(position);
            vector = dictionary.getVector();

            // Dictionary entries can be null
            if (vector.isNull(position)) {
                generator.writeNull();
                return;
            }
        }

        if (extension != null) {
            switch (extension) {
                case "arrow.json" -> {
                    writeJsonExtensionValue(vector, position, generator);
                    return;
                }
                // Other canonical extensions: uuid, tensors, opaque, 8-bit boolean
                // See https://arrow.apache.org/docs/format/CanonicalExtensions.html
                //
                // TODO: GeoArrow (non canonical)
                // See https://geoarrow.org/
            }
        }

        // Use an expression switch to make sure the compiler checks that every enumeration member is used.

        Void x = switch (vector.getMinorType()) {

            //---- Numbers
            // Performance: we could have cast the vector to the common BaseIntVector/FloatingPoint interface,
            // but this would cause more costly casts and polymorphic dispatch to access the value, whereas
            // concrete classes are final, allowing better optimizations or even inlining.
            case TINYINT -> {
                generator.writeNumber(((TinyIntVector) vector).get(position));
                yield null;
            }

            case SMALLINT -> {
                generator.writeNumber(((SmallIntVector) vector).get(position));
                yield null;
            }

            case INT -> {
                generator.writeNumber(((IntVector) vector).getValueAsLong(position));
                yield null;
            }

            case BIGINT -> {
                generator.writeNumber(((BigIntVector) vector).get(position));
                yield null;
            }

            case UINT1 -> {
                generator.writeNumber(((UInt1Vector) vector).getValueAsLong(position));
                yield null;
            }

            case UINT2 -> {
                generator.writeNumber(((UInt2Vector) vector).get(position));
                yield null;
            }

            case UINT4 -> {
                // Use valueAsLong to have unsigned integers if the value is greater than 0x7FFF_FFFF
                generator.writeNumber(((UInt4Vector) vector).getValueAsLong(position));
                yield null;
            }

            case UINT8 -> {
                generator.writeNumber(((UInt8Vector) vector).get(position));
                yield null;
            }

            case FLOAT2 -> {
                generator.writeNumber(((Float2Vector) vector).getValueAsFloat(position));
                yield null;
            }

            case FLOAT4 -> {
                generator.writeNumber(((Float4Vector) vector).get(position));
                yield null;
            }

            case FLOAT8 -> {
                generator.writeNumber(((Float8Vector) vector).get(position));
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
                var buffer = getBuffer(bytesVector.getValueLength(position));
                bytesVector.read(position, buffer);
                generator.writeUTF8String(buffer.getBuffer(), 0, (int)buffer.getLength());
                yield null;
            }

            //---- Binary

            case VARBINARY, LARGEVARBINARY, VIEWVARBINARY -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                var buffer = getBuffer(bytesVector.getValueLength(position));
                bytesVector.read(position, buffer);
                generator.writeBinary(buffer.getBuffer(), 0, (int)buffer.getLength());
                yield null;
            }

            case FIXEDSIZEBINARY -> {
                var bytesVector = (FixedSizeBinaryVector) vector;
                var buffer = getBuffer(bytesVector.getByteWidth());
                bytesVector.read(position, buffer);
                generator.writeBinary(buffer.getBuffer(), 0, (int)buffer.getLength());
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
            // Time since the epoch, in days or millis evenly divisible by 86_400_000
            // Stored as millis

            case DATEDAY -> {
                var ddVector = (DateDayVector) vector;
                generator.writeNumber(ddVector.get(position) * 86_400_000);
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
                    writeValue(valueVector, i, dictionaries, generator);
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

                var keyVector = (VarCharVector) kVector;
                var valueVector = structVector.getChildrenFromFields().get(1);

                int start = mapVector.getElementStartIndex(position);
                int end = mapVector.getElementEndIndex(position);

                generator.writeStartObject();
                for (int i = start; i < end; i++) {
                    if (keyVector.isNull(i)) {
                        throw new ArrowFormatException("Null map key found at position [" + position + "]");
                    }
                    var key = new String(keyVector.get(i), StandardCharsets.UTF_8);
                    generator.writeFieldName(key);
                    writeValue(valueVector, i, dictionaries, generator);
                }
                generator.writeEndObject();
                yield null;
            }

            case STRUCT -> {
                var structVector = (StructVector) vector;
                generator.writeStartObject();
                for (var structField : structVector.getChildrenFromFields()) {
                    generator.writeFieldName(structField.getName());
                    writeValue(structField, position, dictionaries, generator);
                }
                generator.writeEndObject();
                yield null;
            }

            case DENSEUNION -> {
                var unionVector = (DenseUnionVector) vector;
                var typeId = unionVector.getTypeId(position);
                var valueVector = unionVector.getVectorByType(typeId);
                var valuePosition = unionVector.getOffset(position);

                writeValue(valueVector, valuePosition, dictionaries, generator);
                yield null;
            }

            case UNION -> { // sparse union
                var unionVector = (UnionVector) vector;
                var typeId = unionVector.getTypeValue(position);
                var valueVector = unionVector.getVectorByType(typeId);

                writeValue(valueVector, position, dictionaries, generator);
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
                writeValue(reVector.getValuesVector(), reVector.getRunEnd(position), dictionaries, generator);
                yield null;
            }

            case EXTENSIONTYPE -> throw new JsonParseException(
                    "Arrow extension [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
            );
        };
    }

    private static void writeJsonExtensionValue(ValueVector vector, int position, XContentGenerator generator) throws IOException {
        if (STRING_TYPES.contains(vector.getMinorType()) == false) {
            throw new ArrowFormatException("Json vectors must be strings");
        }
        // Parse directly from the Arrow buffer wrapped in a ByteBuffer
        var pointer = ((VariableWidthFieldVector) vector).getDataPointer(position);
        var buf = pointer.getBuf().nioBuffer(pointer.getOffset(), (int)pointer.getLength());

        var parser = XContentType.JSON.xContent().createParser(
            XContentParserConfiguration.EMPTY,
            new ByteBufferBackedInputStream(buf)
        );

        generator.copyCurrentStructure(parser);
    }
}
