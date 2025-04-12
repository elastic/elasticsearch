/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.xcontent;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.FloatingPointVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
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
import org.elasticsearch.libs.arrow.ArrowFormatException;
import org.elasticsearch.xcontent.XContentGenerator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.HashMap;
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

    static final long MILLIS_PER_SEC = 1_000L;
    static final long NANOS_PER_SEC = 1_000_000_000L;
    static final long NANOS_PER_MILLI = 1_000_000L;
    static final long NANOS_PER_MICRO = 1_000L;

    private final Map<String, ZoneId> zidCache = new HashMap<>();

    final long getUTCOffsetSeconds(long millis, String tz) {
        var tzId = zidCache.computeIfAbsent(tz, ZoneId::of);
        if (tzId instanceof ZoneOffset zo) {
            return zo.getTotalSeconds();
        }
        var instant = Instant.ofEpochMilli(millis);
        return tzId.getRules().getOffset(instant).getTotalSeconds();
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

        var dictEncoding = vector.getField().getDictionary();
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

        Void x = switch (vector.getMinorType()) {

            // ----- Primitive values

            case BIT -> {
                generator.writeBoolean(((BitVector) vector).get(position) != 0);
                yield null;
            }

            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4, UINT8 -> {
                generator.writeNumber(((BaseIntVector) vector).getValueAsLong(position));
                yield null;
            }

            case FLOAT2, FLOAT4, FLOAT8 -> {
                generator.writeNumber(((FloatingPointVector) vector).getValueAsDouble(position));
                yield null;
            }

            // ----- Strings and bytes

            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var bytesVector = (VariableWidthFieldVector) vector;
                generator.writeString(new String(bytesVector.get(position), StandardCharsets.UTF_8));
                yield null;
            }

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

            // ----- Lists

            case LIST, FIXED_SIZE_LIST, LISTVIEW -> {
                var listVector = (BaseListVector) vector;
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

            // ----- Time
            //
            // "Time is either a 32-bit or 64-bit signed integer type representing an
            // elapsed time since midnight, stored in either of four units: seconds,
            // milliseconds, microseconds or nanoseconds."
            //
            // There's no such type in ES. Convert it to either milliseconds or nanoseconds to avoid losing precision.
            case TIMESEC -> {
                var tsVector = (TimeSecVector) vector;
                generator.writeNumber(tsVector.get(position) * MILLIS_PER_SEC); // millisecs
                yield null;
            }

            case TIMEMILLI -> {
                var tsVector = (TimeMilliVector) vector;
                generator.writeNumber(tsVector.get(position)); // millisecs
                yield null;
            }

            case TIMEMICRO -> {
                var tsVector = (TimeMicroVector) vector;
                generator.writeNumber(tsVector.get(position) * NANOS_PER_MICRO); // nanosecs
                yield null;
            }

            case TIMENANO -> {
                var tsVector = (TimeNanoVector) vector;
                generator.writeNumber(tsVector.get(position)); // nanosecs
                yield null;
            }

            // ----- Timestamp
            //
            // From the spec: "Timestamp is a 64-bit signed integer representing an elapsed time since a
            // fixed epoch, stored in either of four units: seconds, milliseconds,
            // microseconds or nanoseconds, and is optionally annotated with a timezone.
            // If a Timestamp column has no timezone value, its epoch is
            // 1970-01-01 00:00:00 (January 1st 1970, midnight) in an *unknown* timezone."
            //
            // Arrow/Java uses different types for timestamps with a timezone (TIMESTAMPXXXTZ) and without
            // a timezone (TIMESTAMPXXX).
            // ES doesn't support timezones, so the TIMESTAMPXXXTZ are not supported.

            case TIMESTAMPSEC -> {
                var tsVector = (TimeStampSecVector) vector;
                generator.writeNumber(tsVector.get(position) * MILLIS_PER_SEC); // millisecs
                yield null;
            }

            case TIMESTAMPMILLI -> {
                var tsVector = (TimeStampMilliVector) vector;
                generator.writeNumber(tsVector.get(position)); // millisecs
                yield null;
            }

            case TIMESTAMPMICRO -> {
                var tsVector = (TimeStampMicroVector) vector;
                generator.writeNumber(tsVector.get(position) * NANOS_PER_MICRO); // nanosecs
                yield null;
            }

            case TIMESTAMPNANO -> {
                var tsVector = (TimeStampNanoVector) vector;
                generator.writeNumber(tsVector.get(position)); // nanosecs
                yield null;
            }

            // ----- Timestamp with a timezone

            case TIMESTAMPSECTZ -> {
                var tsVector = (TimeStampSecTZVector) vector;
                long millis = tsVector.get(position) * MILLIS_PER_SEC;
                millis -= getUTCOffsetSeconds(millis, tsVector.getTimeZone()) * MILLIS_PER_SEC;
                generator.writeNumber(millis);
                yield null;
            }

            case TIMESTAMPMILLITZ -> {
                var tsVector = (TimeStampMilliTZVector) vector;
                long millis = tsVector.get(position);
                millis -= getUTCOffsetSeconds(millis, tsVector.getTimeZone()) * MILLIS_PER_SEC;
                generator.writeNumber(millis);
                yield null;
            }

            case TIMESTAMPMICROTZ -> {
                var tsVector = (TimeStampMicroTZVector) vector;
                long nanos = tsVector.get(position) * NANOS_PER_MICRO;
                nanos -= getUTCOffsetSeconds(nanos / NANOS_PER_MILLI, tsVector.getTimeZone()) * NANOS_PER_SEC;
                generator.writeNumber(nanos);
                yield null;
            }

            case TIMESTAMPNANOTZ -> {
                var tsVector = (TimeStampNanoTZVector) vector;
                long nanos = tsVector.get(position);
                nanos -= getUTCOffsetSeconds(nanos / NANOS_PER_SEC, tsVector.getTimeZone()) * NANOS_PER_SEC;
                generator.writeNumber(nanos);
                yield null;
            }

            // ----- Composite types

            case MAP -> {
                // A map is a container vector composed of a list of struct values with "key" and "value" fields. The MapVector
                // is nullable, but if a map is set at a given index, there must be an entry. In other words, the StructVector data is
                // non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can be null.

                var mapVector = (MapVector) vector;
                var structVector = (StructVector) mapVector.getChildrenFromFields().get(0);
                var kVector = structVector.getChildrenFromFields().get(0);
                if (STRING_TYPES.contains(kVector.getMinorType()) == false) {
                    throw new ArrowFormatException("Arrow maps must have string keys to be converted to JSON");
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
                for (var field : structVector.getChildrenFromFields()) {
                    generator.writeFieldName(field.getName());
                    writeValue(field, position, dictionaries, generator);
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
                generator.writeNull();
                yield null;
            }

            // TODO
            case DATEDAY, DATEMILLI, INTERVALDAY, INTERVALMONTHDAYNANO, DURATION, INTERVALYEAR, DECIMAL, DECIMAL256, LARGELIST,
                LARGELISTVIEW, EXTENSIONTYPE, RUNENDENCODED -> throw new ArrowFormatException(
                    "Arrow type [" + vector.getMinorType() + "] not supported for field [" + vector.getName() + "]"
                );
        };
    }
}
