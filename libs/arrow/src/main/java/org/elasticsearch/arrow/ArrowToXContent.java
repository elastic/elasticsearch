/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.elasticsearch.xcontent.XContentGenerator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;

/**
 * Utility methods to serialize Arrow dataframes to XContent events.
 * <p>
 * Limitations:
 * <ul>
 * <li>time and timestamps are converted to milliseconds (no support for nanoseconds)
 * </li>
 * <li>some types aren't implemented
 * </li>
 * </ul>
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#data-types">Arrow data types</a>
 */
public class ArrowToXContent {

    private static final EnumSet<Types.MinorType> STRING_TYPES = EnumSet.of(
        Types.MinorType.VARCHAR,
        Types.MinorType.LARGEVARCHAR,
        Types.MinorType.VIEWVARCHAR
    );

    /**
     * Write a field and its value from an Arrow vector as XContent
     *
     * @param vector the Arrow vector
     * @param position the value position in the vector
     * @param dictionaries to look up values for dictionary-encoded vectors
     * @param generator XContent output
     */
    public static void writeField(
        ValueVector vector, int position, Map<Long, Dictionary> dictionaries, XContentGenerator generator
    ) throws IOException {
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
    public static void writeValue(
        ValueVector vector, int position, Map<Long, Dictionary> dictionaries, XContentGenerator generator
    ) throws IOException {
        if (vector.isNull(position)) {
            generator.writeNull();
            return;
        }

        var dictEncoding = vector.getField().getDictionary();
        if (dictEncoding != null) {
            // Note: to improve performance and reduce GC thrashing, we could eagerly convert dictionary
            // VarCharVectors to String arrays (likely the most frequent use of dictionaries)
            Dictionary dictionary = dictionaries.get(dictEncoding.getId());
            position = (int) ((BaseIntVector)vector).getValueAsLong(position);
            vector = dictionary.getVector();
        }

        Void x = switch (vector.getMinorType()) {

            //----- Primitive values

            case BIT -> {
                generator.writeBoolean(((BitVector)vector).get(position) != 0);
                yield null;
            }

            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> {
                generator.writeNumber(((BaseIntVector)vector).getValueAsLong(position));
                yield null;
            }

            case FLOAT2, FLOAT4, FLOAT8 -> {
                generator.writeNumber(((FloatingPointVector)vector).getValueAsDouble(position));
                yield null;
            }

            //----- strings and bytes

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector)vector;
                generator.writeString(new String(bytesVector.get(position), StandardCharsets.UTF_8));
                yield null;
            }

            case VARBINARY, LARGEVARBINARY, VIEWVARBINARY -> {
                var bytesVector = (VariableWidthFieldVector)vector;
                generator.writeBinary(bytesVector.get(position));
                yield null;
            }

            case FIXEDSIZEBINARY -> {
                var bytesVector = (FixedSizeBinaryVector)vector;
                generator.writeBinary(bytesVector.get(position));
                yield null;
            }

            //----- lists

            case LIST, FIXED_SIZE_LIST, LISTVIEW -> {
                var listVector = (BaseListVector)vector;
                var valueVector = listVector.getChildrenFromFields().get(0);
                int start = listVector.getElementStartIndex(position);
                int end = listVector.getElementEndIndex(position);

                generator.writeStartArray();
                for (int i = start; i < end; i++) {
                    writeValue(valueVector, i, dictionaries, generator);
                }
                generator.writeEndArray();
                yield null;
            }

            //----- Time & Timestamp (time + timezone)

            // Timestamps are the elapsed time since the Epoch, with an optional timezone that
            // can be used for timezome-aware operations or display. Since ES date fields
            // don't support timezones, we ignore it.
            // See https://github.com/apache/arrow/blob/main/format/Schema.fbs
            // and https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html

            case TIMESEC, TIMESTAMPSEC -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)*1000);
                yield null;
            }

            case TIMEMILLI, TIMESTAMPMILLI -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position));
                yield null;
            }

            case TIMEMICRO, TIMESTAMPMICRO -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)/1000);
                yield null;
            }

            case TIMENANO, TIMESTAMPNANO -> {
                var tsVector = (TimeStampVector)vector;
                generator.writeNumber(tsVector.get(position)/1_000_000);
                yield null;
            }

            //----- Composite types

            case MAP -> {
                // A map is a container vector composed of a list of struct values with "key" and "value" fields. The MapVector
                // is nullable, but if a map is set at a given index, there must be an entry. In other words, the StructVector data is
                // non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can be null.

                var mapVector = (MapVector)vector;
                var structVector = (StructVector)mapVector.getChildrenFromFields().get(0);
                var kVector = structVector.getChildrenFromFields().get(0);
                if (STRING_TYPES.contains(kVector.getMinorType()) == false) {
                    throw new ArrowFormatException("Arrow maps must have string keys to be converted to JSON");
                }

                var keyVector = (VarCharVector)kVector;
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
                var structVector = (StructVector)vector;
                generator.writeStartObject();
                for (var field: structVector.getChildrenFromFields()) {
                    generator.writeFieldName(field.getName());
                    writeValue(field, position, dictionaries, generator);
                }
                generator.writeEndObject();
                yield null;
            }

            case DENSEUNION -> {
                var unionVector = (DenseUnionVector)vector;
                var typeId = unionVector.getTypeId(position);
                var valueVector = unionVector.getVectorByType(typeId);
                var valuePosition = unionVector.getOffset(position);

                writeValue(valueVector, valuePosition, dictionaries, generator);
                yield null;
            }

            case UNION -> { // sparse union
                var unionVector = (UnionVector)vector;
                var typeId = unionVector.getTypeValue(position);
                var valueVector = unionVector.getVectorByType(typeId);

                writeValue(valueVector, position, dictionaries, generator);
                yield null;
            }

            case NULL -> {
                generator.writeNull();
                yield null;
            }

            // TODO
            case DATEDAY,
                 DATEMILLI,
                 INTERVALDAY,
                 INTERVALMONTHDAYNANO,
                 DURATION,
                 INTERVALYEAR,
                 DECIMAL,
                 DECIMAL256,
                 LARGELIST,
                 LARGELISTVIEW,
                 TIMESTAMPSECTZ,
                 TIMESTAMPMILLITZ,
                 TIMESTAMPMICROTZ,
                 TIMESTAMPNANOTZ,
                 EXTENSIONTYPE,
                 RUNENDENCODED -> throw new ArrowFormatException(
                    "Arrow type [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
                );
        };
    }
}
