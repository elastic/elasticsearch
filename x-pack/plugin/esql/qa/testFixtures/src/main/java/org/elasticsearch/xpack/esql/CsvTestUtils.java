/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BlockUtils.BuilderWrapper;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.Strings.delimitedListToStringArray;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.reader;
import static org.elasticsearch.xpack.esql.SpecReader.shouldSkipLine;
import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.ISO_DATE_WITH_NANOS;
import static org.elasticsearch.xpack.esql.core.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.asLongUnsigned;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;

public final class CsvTestUtils {
    private static final int MAX_WIDTH = 20;
    private static final CsvPreference CSV_SPEC_PREFERENCES = new CsvPreference.Builder('"', '|', "\r\n").build();
    private static final String NULL_VALUE = "null";
    private static final char ESCAPE_CHAR = '\\';
    public static final String COMMA_ESCAPING_REGEX = "(?<!\\" + ESCAPE_CHAR + "),";
    public static final String ESCAPED_COMMA_SEQUENCE = ESCAPE_CHAR + ",";

    private CsvTestUtils() {}

    public static boolean isEnabled(String testName, String instructions, Version version) {
        if (testName.endsWith("-Ignore")) {
            return false;
        }
        Tuple<Version, Version> skipRange = skipVersionRange(testName, instructions);
        if (skipRange != null && version.onOrAfter(skipRange.v1()) && version.onOrBefore(skipRange.v2())) {
            return false;
        }
        return true;
    }

    private static final Pattern INSTRUCTION_PATTERN = Pattern.compile("\\[(.*?)]");

    public static Map<String, String> parseInstructions(String instructions) {
        Matcher matcher = INSTRUCTION_PATTERN.matcher(instructions);
        Map<String, String> pairs = new HashMap<>();
        if (matcher.find()) {
            String[] groups = matcher.group(1).split(",");
            for (String group : groups) {
                String[] kv = group.split(":");
                if (kv.length != 2) {
                    throw new IllegalArgumentException("expected instruction in [k1:v1,k2:v2] format; got " + matcher.group(1));
                }
                pairs.put(kv[0].trim(), kv[1].trim());
            }
        }
        return pairs;
    }

    public static Tuple<Version, Version> skipVersionRange(String testName, String instructions) {
        Map<String, String> pairs = parseInstructions(instructions);
        String versionRange = pairs.get("skip");
        if (versionRange != null) {
            String[] skipVersions = versionRange.split("-", Integer.MAX_VALUE);
            if (skipVersions.length != 2) {
                throw new IllegalArgumentException("malformed version range : " + versionRange);
            }
            String lower = skipVersions[0].trim();
            String upper = skipVersions[1].trim();
            return Tuple.tuple(
                lower.isEmpty() ? VersionUtils.getFirstVersion() : Version.fromString(lower),
                upper.isEmpty() ? Version.CURRENT : Version.fromString(upper)
            );
        }
        return null;
    }

    public static Tuple<Page, List<String>> loadPageFromCsv(URL source) throws Exception {

        record CsvColumn(String name, Type type, BuilderWrapper builderWrapper) implements Releasable {
            void append(String stringValue) {
                if (stringValue.startsWith("\"") && stringValue.endsWith("\"")) { // string value
                    stringValue = stringValue.substring(1, stringValue.length() - 1).replace(ESCAPED_COMMA_SEQUENCE, ",");
                } else if (stringValue.contains(",")) {// multi-value field
                    builderWrapper().builder().beginPositionEntry();

                    String[] arrayOfValues = delimitedListToStringArray(stringValue, ",");
                    List<Object> convertedValues = new ArrayList<>(arrayOfValues.length);
                    for (String value : arrayOfValues) {
                        convertedValues.add(type.convert(value));
                    }
                    convertedValues.stream().sorted().forEach(v -> builderWrapper().append().accept(v));
                    builderWrapper().builder().endPositionEntry();

                    return;
                }

                var converted = stringValue.length() == 0 ? null : type.convert(stringValue);
                builderWrapper().append().accept(converted);
            }

            @Override
            public void close() {
                builderWrapper.close();
            }
        }

        CsvColumn[] columns = null;

        var blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        try (BufferedReader reader = reader(source)) {
            String line;
            int lineNumber = 1;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (shouldSkipLine(line) == false) {
                    String[] entries = multiValuesAwareCsvToStringArray(line, lineNumber);
                    // the schema row
                    if (columns == null) {
                        columns = new CsvColumn[entries.length];
                        for (int i = 0; i < entries.length; i++) {
                            int split = entries[i].indexOf(':');
                            String name, typeName;

                            if (split < 0) {
                                throw new IllegalArgumentException(
                                    "A type is always expected in the schema definition; found " + entries[i]
                                );
                            } else {
                                name = entries[i].substring(0, split).trim();
                                typeName = entries[i].substring(split + 1).trim();
                                if (typeName.length() == 0) {
                                    throw new IllegalArgumentException(
                                        "A type is always expected in the schema definition; found " + entries[i]
                                    );
                                }
                            }
                            Type type = Type.asType(typeName);
                            if (type == null) {
                                throw new IllegalArgumentException("Can't find type for " + entries[i]);
                            }
                            if (type == Type.NULL) {
                                throw new IllegalArgumentException("Null type is not allowed in the test data; found " + entries[i]);
                            }
                            columns[i] = new CsvColumn(
                                name,
                                type,
                                BlockUtils.wrapperFor(blockFactory, ElementType.fromJava(type.clazz()), 8)
                            );
                        }
                    }
                    // data rows
                    else {
                        if (entries.length != columns.length) {
                            throw new IllegalArgumentException(
                                format(
                                    "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                    lineNumber,
                                    columns.length,
                                    entries.length
                                )
                            );
                        }
                        for (int i = 0; i < entries.length; i++) {
                            var entry = entries[i];
                            try {
                                columns[i].append(entry);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(
                                    format("Error line [{}]: Cannot parse entry [{}] with value [{}]", lineNumber, i + 1, entry),
                                    e
                                );
                            }
                        }
                    }
                }
                lineNumber++;
            }
        }
        var columnNames = new ArrayList<String>(columns.length);
        try {
            var blocks = Arrays.stream(columns)
                .peek(b -> columnNames.add(b.name))
                .map(b -> b.builderWrapper.builder().build())
                .toArray(Block[]::new);
            return new Tuple<>(new Page(blocks), columnNames);
        } finally {
            Releasables.closeExpectNoException(columns);
        }
    }

    /**
     * Takes a csv String and converts it to a String array. Also, it recognizes an opening bracket "[" in one string and a closing "]"
     * in another string and it creates a single concatenated comma-separated String of all the values between the opening bracket entry
     * and the closing bracket entry. In other words, entries enclosed by "[]" are returned as a single element.
     *
     * Commas can be escaped with \ (backslash) character.
     */
    static String[] multiValuesAwareCsvToStringArray(String csvLine, int lineNumber) {
        var mvCompressedEntries = new ArrayList<String>();
        String previousMvValue = null; // just helping out with error messaging
        StringBuilder mvValue = null;

        int pos = 0;          // current position in the csv String
        int commaPos;         // current "," character position
        int previousCommaPos = 0;
        while ((commaPos = csvLine.indexOf(',', pos)) != -1 || pos <= csvLine.length()) {
            if (commaPos > 0 && csvLine.charAt(commaPos - 1) == ESCAPE_CHAR) {// skip the escaped comma
                pos = commaPos + 1;// moving on to the next character after comma
                continue;
            }

            boolean isLastElement = commaPos == -1;
            String entry = csvLine.substring(previousCommaPos, isLastElement ? csvLine.length() : commaPos).trim();
            if (entry.startsWith("[")) {
                if (previousMvValue != null || (isLastElement && entry.endsWith("]") == false)) {
                    String message = "Error line [{}:{}]: Unexpected start of a multi-value field value; current token [{}], "
                        + (isLastElement ? "no closing point" : "previous token [{}]");
                    throw new IllegalArgumentException(format(message, lineNumber, previousCommaPos, entry, previousMvValue));
                }
                if (entry.endsWith("]")) {
                    if (entry.length() > 2) {// single-valued multivalue field :shrug:
                        mvCompressedEntries.add(entry.substring(1, entry.length() - 1));
                    } else {// empty multivalue field
                        mvCompressedEntries.add("");
                    }
                } else {
                    mvValue = new StringBuilder();
                    previousMvValue = entry.substring(1);
                    mvValue.append(previousMvValue);
                }
            } else if (entry.endsWith("]")) {
                if (previousMvValue == null) {
                    throw new IllegalArgumentException(
                        format(
                            "Error line [{}:{}]: Unexpected end of a multi-value field value (no previous starting point); found [{}]",
                            lineNumber,
                            previousCommaPos,
                            entry
                        )
                    );
                }
                mvValue.append("," + entry.substring(0, entry.length() - 1));
                mvCompressedEntries.add(mvValue.toString());
                mvValue = null;
                previousMvValue = null;
            } else {
                if (mvValue != null) {// mid-MV value
                    if (entry.length() == 0) {// this means there shouldn't be any null value in a multi-value field ie [a,,b,c]
                        throw new IllegalArgumentException(
                            format(
                                "Error line [{}:{}]: Unexpected missing value in a multi-value column; found [{}]",
                                lineNumber,
                                previousCommaPos,
                                csvLine.substring(previousCommaPos - 1)
                            )
                        );
                    }
                    mvValue.append("," + entry);
                } else {
                    mvCompressedEntries.add(entry);// regular comma separated value
                }
            }
            pos = 1 + (isLastElement ? csvLine.length() : commaPos);// break out of the loop if it reached its last element
            previousCommaPos = pos;
        }
        return mvCompressedEntries.toArray(String[]::new);
    }

    public record ExpectedResults(List<String> columnNames, List<Type> columnTypes, List<List<Object>> values) {}

    /**
     * The method loads a section of a .csv-spec file representing the results of executing the query of that section.
     * It reads both the schema (field names and their types) and the row values.
     * Values starting with an opening square bracket and ending with a closing square bracket are considered multi-values. Inside
     * these multi-values, commas separate the individual values and escaped commas are allowed with a prefixed \
     * default \ (backslash) character.
     * @param csv a string representing the header and row values of a single query execution result
     * @return data structure with column names, their types and values
     */
    public static ExpectedResults loadCsvSpecValues(String csv) {
        List<String> columnNames;
        List<Type> columnTypes;

        try (CsvListReader listReader = new CsvListReader(new StringReader(csv), CSV_SPEC_PREFERENCES)) {
            String[] header = listReader.getHeader(true);
            columnNames = new ArrayList<>(header.length);
            columnTypes = new ArrayList<>(header.length);

            for (String c : header) {
                String[] nameWithType = escapeTypecast(c).split(":");
                if (nameWithType.length != 2) {
                    throw new IllegalArgumentException("Invalid CSV header " + c);
                }
                String typeName = unescapeTypecast(nameWithType[1]).trim();
                if (typeName.isEmpty()) {
                    throw new IllegalArgumentException("A type is always expected in the csv file; found " + Arrays.toString(nameWithType));
                }
                String name = unescapeTypecast(nameWithType[0]).trim();
                columnNames.add(name);
                Type type = Type.asType(typeName);
                if (type == null) {
                    throw new IllegalArgumentException("Unknown type name: [" + typeName + "]");
                }
                columnTypes.add(type);
            }

            List<List<Object>> values = new ArrayList<>();
            List<String> row;
            while ((row = listReader.read()) != null) {
                List<Object> rowValues = new ArrayList<>(row.size());
                for (int i = 0; i < row.size(); i++) {
                    String value = row.get(i);
                    if (value == null) {
                        // Empty cells are converted to null by SuperCSV. We convert them back to empty strings.
                        rowValues.add("");
                        continue;
                    }

                    value = value.trim();
                    if (value.equalsIgnoreCase(NULL_VALUE)) {
                        rowValues.add(null);
                        continue;
                    }
                    if (value.startsWith("[")) {
                        if (false == value.endsWith("]")) {
                            throw new IllegalArgumentException(
                                "Incomplete multi-value (opening and closing square brackets) found " + value + " on row " + values.size()
                            );
                        }
                        // split on commas but ignoring escaped commas
                        String[] multiValues = value.substring(1, value.length() - 1).split(COMMA_ESCAPING_REGEX);
                        if (multiValues.length == 1) {
                            rowValues.add(columnTypes.get(i).convert(multiValues[0].replace(ESCAPED_COMMA_SEQUENCE, ",")));
                            continue;
                        }
                        List<Object> listOfMvValues = new ArrayList<>();
                        for (String mvValue : multiValues) {
                            listOfMvValues.add(columnTypes.get(i).convert(mvValue.trim().replace(ESCAPED_COMMA_SEQUENCE, ",")));
                        }
                        rowValues.add(listOfMvValues);
                    } else {
                        // The value considered here is the one where any potential escaped comma is kept as is (with the escape char)
                        // TODO if we'd want escaped commas outside multi-values fields, we'd have to adjust this value here as well
                        rowValues.add(columnTypes.get(i).convert(value));
                    }
                }
                values.add(rowValues);
            }

            return new ExpectedResults(columnNames, columnTypes, values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String TYPECAST_SPACER = "__TYPECAST__";

    private static String escapeTypecast(String typecast) {
        return typecast.replace("::", TYPECAST_SPACER);
    }

    private static String unescapeTypecast(String typecast) {
        return typecast.replace(TYPECAST_SPACER, "::");
    }

    public enum Type {
        INTEGER(Integer::parseInt, Integer.class),
        LONG(Long::parseLong, Long.class),
        UNSIGNED_LONG(s -> asLongUnsigned(safeToUnsignedLong(s)), Long.class),
        DOUBLE(Double::parseDouble, Double.class),
        FLOAT(
            // Simulate writing the index as `float` precision by parsing as a float and rounding back to double
            s -> (double) Float.parseFloat(s),
            Double.class
        ),
        HALF_FLOAT(
            s -> (double) HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(Float.parseFloat(s))),
            Double.class
        ),
        // we currently only support a hard-coded scaling factor, since we're not querying the mapping of a field when reading CSV values
        // for it, so the scaling_factor isn't available
        SCALED_FLOAT(s -> s == null ? null : scaledFloat(s, "100"), Double.class),
        KEYWORD(Object::toString, BytesRef.class),
        TEXT(Object::toString, BytesRef.class),
        IP(
            StringUtils::parseIP,
            (l, r) -> l instanceof String maybeIP
                ? StringUtils.parseIP(maybeIP).compareTo(StringUtils.parseIP(String.valueOf(r)))
                : ((BytesRef) l).compareTo((BytesRef) r),
            BytesRef.class
        ),
        IP_RANGE(InetAddresses::parseCidr, BytesRef.class),
        INTEGER_RANGE(s -> s == null ? null : Arrays.stream(s.split("-")).map(Integer::parseInt).toArray(), int[].class),
        DOUBLE_RANGE(s -> s == null ? null : Arrays.stream(s.split("-")).map(Double::parseDouble).toArray(), double[].class),
        DATE_RANGE(s -> s == null ? null : Arrays.stream(s.split("-")).map(BytesRef::new).toArray(), BytesRef[].class),
        VERSION(v -> new org.elasticsearch.xpack.versionfield.Version(v).toBytesRef(), BytesRef.class),
        NULL(s -> null, Void.class),
        DATETIME(
            x -> x == null ? null : DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(x)).toInstant().toEpochMilli(),
            (l, r) -> l instanceof Long maybeIP ? maybeIP.compareTo((Long) r) : l.toString().compareTo(r.toString()),
            Long.class
        ),
        DATE_NANOS(x -> {
            if (x == null) {
                return null;
            }
            Instant parsed = DateFormatters.from(ISO_DATE_WITH_NANOS.parse(x)).toInstant();
            return parsed.getEpochSecond() * 1_000_000_000 + parsed.getNano();
        }, (l, r) -> l instanceof Long maybeIP ? maybeIP.compareTo((Long) r) : l.toString().compareTo(r.toString()), Long.class),
        BOOLEAN(Booleans::parseBoolean, Boolean.class),
        GEO_POINT(x -> x == null ? null : GEO.wktToWkb(x), BytesRef.class),
        CARTESIAN_POINT(x -> x == null ? null : CARTESIAN.wktToWkb(x), BytesRef.class),
        GEO_SHAPE(x -> x == null ? null : GEO.wktToWkb(x), BytesRef.class),
        CARTESIAN_SHAPE(x -> x == null ? null : CARTESIAN.wktToWkb(x), BytesRef.class);

        private static final Map<String, Type> LOOKUP = new HashMap<>();

        static {
            for (Type value : Type.values()) {
                LOOKUP.put(value.name(), value);
            }
            // widen smaller types
            LOOKUP.put("SHORT", INTEGER);
            LOOKUP.put("BYTE", INTEGER);

            // counter types
            LOOKUP.put("COUNTER_INTEGER", INTEGER);
            LOOKUP.put("COUNTER_LONG", LONG);
            LOOKUP.put("COUNTER_DOUBLE", DOUBLE);
            LOOKUP.put("COUNTER_FLOAT", FLOAT);

            // add also the types with short names
            LOOKUP.put("BOOL", BOOLEAN);
            LOOKUP.put("I", INTEGER);
            LOOKUP.put("L", LONG);
            LOOKUP.put("UL", UNSIGNED_LONG);
            LOOKUP.put("D", DOUBLE);
            LOOKUP.put("K", KEYWORD);
            LOOKUP.put("S", KEYWORD);
            LOOKUP.put("STRING", KEYWORD);
            LOOKUP.put("TXT", TEXT);
            LOOKUP.put("N", NULL);
            LOOKUP.put("DATE", DATETIME);
            LOOKUP.put("DT", DATETIME);
            LOOKUP.put("V", VERSION);
        }

        private final Function<String, Object> converter;
        private final Class<?> clazz;
        private final Comparator<Object> comparator;

        @SuppressWarnings("unchecked")
        Type(Function<String, Object> converter, Class<?> clazz) {
            this(
                converter,
                Comparable.class.isAssignableFrom(clazz) ? (a, b) -> ((Comparable) a).compareTo(b) : Comparator.comparing(Object::toString),
                clazz
            );
        }

        Type(Function<String, Object> converter, Comparator<Object> comparator, Class<?> clazz) {
            this.converter = converter;
            this.comparator = comparator;
            this.clazz = clazz;
        }

        public static Type asType(String name) {
            return LOOKUP.get(name.toUpperCase(Locale.ROOT));
        }

        public static Type asType(ElementType elementType, Type actualType) {
            return switch (elementType) {
                case INT -> INTEGER;
                case LONG -> LONG;
                case FLOAT -> FLOAT;
                case DOUBLE -> DOUBLE;
                case NULL -> NULL;
                case BYTES_REF -> bytesRefBlockType(actualType);
                case BOOLEAN -> BOOLEAN;
                case DOC -> throw new IllegalArgumentException("can't assert on doc blocks");
                case COMPOSITE -> throw new IllegalArgumentException("can't assert on composite blocks");
                case UNKNOWN -> throw new IllegalArgumentException("Unknown block types cannot be handled");
            };
        }

        private static Type bytesRefBlockType(Type actualType) {
            if (actualType == GEO_POINT || actualType == CARTESIAN_POINT || actualType == GEO_SHAPE || actualType == CARTESIAN_SHAPE) {
                return actualType;
            } else {
                return KEYWORD;
            }
        }

        Object convert(String value) {
            if (value == null) {
                return null;
            }
            return converter.apply(value);
        }

        Class<?> clazz() {
            return clazz;
        }

        public Comparator<Object> comparator() {
            return comparator;
        }
    }

    record ActualResults(
        List<String> columnNames,
        List<Type> columnTypes,
        List<DataType> dataTypes,
        List<Page> pages,
        Map<String, List<String>> responseHeaders
    ) {
        Iterator<Iterator<Object>> values() {
            return ResponseValueUtils.pagesToValues(dataTypes(), pages);
        }
    }

    public static void logMetaData(List<String> actualColumnNames, List<Type> actualColumnTypes, Logger logger) {
        // header
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();

        for (int i = 0; i < actualColumnNames.size(); i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            column.setLength(0);
            column.append(actualColumnNames.get(i));
            column.append("(");
            column.append(actualColumnTypes.get(i));
            column.append(")");

            sb.append(trimOrPad(column));
        }

        int l = sb.length();
        logger.info(sb.toString());
        sb.setLength(0);
        sb.append("-".repeat(Math.max(0, l)));

        logger.info(sb.toString());
    }

    static void logData(Iterator<Iterator<Object>> values, Logger logger) {
        while (values.hasNext()) {
            var val = values.next();
            logger.info(rowAsString(val));
        }
    }

    private static String rowAsString(Iterator<Object> iterator) {
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();
        for (int i = 0; iterator.hasNext(); i++) {
            column.setLength(0);
            if (i > 0) {
                sb.append(" | ");
            }
            var next = iterator.next();
            sb.append(trimOrPad(column.append(next)));
        }
        return sb.toString();
    }

    private static StringBuilder trimOrPad(StringBuilder buffer) {
        if (buffer.length() > MAX_WIDTH) {
            buffer.setLength(MAX_WIDTH - 1);
            buffer.append("~");
        } else {
            buffer.append(" ".repeat(Math.max(0, MAX_WIDTH - buffer.length())));
        }
        return buffer;
    }

    private static double scaledFloat(String value, String factor) {
        double scalingFactor = Double.parseDouble(factor);
        // this extra division introduces extra imprecision in the following multiplication, but this is how ScaledFloatFieldMapper works.
        double scalingFactorInverse = 1d / scalingFactor;
        return new BigDecimal(value).multiply(BigDecimal.valueOf(scalingFactor)).longValue() * scalingFactorInverse;
    }
}
