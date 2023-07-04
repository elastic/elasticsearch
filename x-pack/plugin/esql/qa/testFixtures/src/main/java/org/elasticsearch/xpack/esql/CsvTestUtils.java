/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BlockUtils.BuilderWrapper;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.Strings.delimitedListToStringArray;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.SpecReader.shouldSkipLine;
import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;
import static org.elasticsearch.xpack.ql.util.NumericUtils.asLongUnsigned;

public final class CsvTestUtils {
    private static final int MAX_WIDTH = 20;
    private static final CsvPreference CSV_SPEC_PREFERENCES = new CsvPreference.Builder('"', '|', "\r\n").build();
    private static final String NULL_VALUE = "null";

    private CsvTestUtils() {}

    public static boolean isEnabled(String testName) {
        return testName.endsWith("-Ignore") == false;
    }

    public static Tuple<Page, List<String>> loadPageFromCsv(URL source) throws Exception {

        record CsvColumn(String name, Type type, BuilderWrapper builderWrapper) {
            void append(String stringValue) {
                if (stringValue.contains(",")) {// multi-value field
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
        }

        CsvColumn[] columns = null;

        try (BufferedReader reader = org.elasticsearch.xpack.ql.TestUtils.reader(source)) {
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
                            int split = entries[i].indexOf(":");
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
                            columns[i] = new CsvColumn(name, type, BlockUtils.wrapperFor(type.clazz(), 8));
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
        var blocks = Arrays.stream(columns)
            .peek(b -> columnNames.add(b.name))
            .map(b -> b.builderWrapper.builder().build())
            .toArray(Block[]::new);
        return new Tuple<>(new Page(blocks), columnNames);
    }

    /**
     * Takes a csv String and converts it to a String array. Also, it recognizes an opening bracket "[" in one string and a closing "]"
     * in another string and it creates a single concatenated comma-separated String of all the values between the opening bracket entry
     * and the closing bracket entry. In other words, entries enclosed by "[]" are returned as a single element.
     */
    static String[] multiValuesAwareCsvToStringArray(String csvLine, int lineNumber) {
        var mvCompressedEntries = new ArrayList<String>();
        String previousMvValue = null; // just helping out with error messaging
        StringBuilder mvValue = null;

        int pos = 0;          // current position in the csv String
        int commaPos;         // current "," character position
        while ((commaPos = csvLine.indexOf(",", pos)) != -1 || pos <= csvLine.length()) {
            boolean isLastElement = commaPos == -1;
            String entry = csvLine.substring(pos, isLastElement ? csvLine.length() : commaPos).trim();
            if (entry.startsWith("[")) {
                if (previousMvValue != null || (isLastElement && entry.endsWith("]") == false)) {
                    String message = "Error line [{}:{}]: Unexpected start of a multi-value field value; current token [{}], "
                        + (isLastElement ? "no closing point" : "previous token [{}]");
                    throw new IllegalArgumentException(format(message, lineNumber, pos, entry, previousMvValue));
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
                            pos,
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
                                pos,
                                csvLine.substring(pos - 1)
                            )
                        );
                    }
                    mvValue.append("," + entry);
                } else {
                    mvCompressedEntries.add(entry);// regular comma separated value
                }
            }
            pos = 1 + (isLastElement ? csvLine.length() : commaPos);// break out of the loop if it reached its last element
        }
        return mvCompressedEntries.toArray(String[]::new);
    }

    public record ExpectedResults(List<String> columnNames, List<Type> columnTypes, List<List<Object>> values) {}

    public static ExpectedResults loadCsvSpecValues(String csv) {
        List<String> columnNames;
        List<Type> columnTypes;

        try (CsvListReader listReader = new CsvListReader(new StringReader(csv), CSV_SPEC_PREFERENCES)) {
            String[] header = listReader.getHeader(true);
            columnNames = new ArrayList<>(header.length);
            columnTypes = new ArrayList<>(header.length);

            for (String c : header) {
                String[] nameWithType = Strings.split(c, ":");
                if (nameWithType == null || nameWithType.length != 2) {
                    throw new IllegalArgumentException("Invalid CSV header " + c);
                }
                String typeName = nameWithType[1].trim();
                if (typeName.length() == 0) {
                    throw new IllegalArgumentException("A type is always expected in the csv file; found " + nameWithType);
                }
                String name = nameWithType[0].trim();
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
                    if (value == null || value.trim().equalsIgnoreCase(NULL_VALUE)) {
                        value = null;
                        rowValues.add(columnTypes.get(i).convert(value));
                        continue;
                    }

                    value = value.trim();
                    if (value.startsWith("[") ^ value.endsWith("]")) {
                        throw new IllegalArgumentException("Incomplete multi-value (opening and closing square brackets) found " + value);
                    }
                    if (value.contains(",") && value.startsWith("[")) {// commas outside a multi-value should be ok
                        List<Object> listOfMvValues = new ArrayList<>();
                        for (String mvValue : delimitedListToStringArray(value.substring(1, value.length() - 1), ",")) {
                            listOfMvValues.add(columnTypes.get(i).convert(mvValue.trim()));
                        }
                        rowValues.add(listOfMvValues);
                    } else {
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
        IP(StringUtils::parseIP, BytesRef.class),
        VERSION(v -> new Version(v).toBytesRef(), BytesRef.class),
        NULL(s -> null, Void.class),
        DATETIME(x -> x == null ? null : DateFormatters.from(UTC_DATE_TIME_FORMATTER.parse(x)).toInstant().toEpochMilli(), Long.class),
        BOOLEAN(Booleans::parseBoolean, Boolean.class);

        private static final Map<String, Type> LOOKUP = new HashMap<>();

        static {
            for (Type value : Type.values()) {
                LOOKUP.put(value.name(), value);
            }
            // widen smaller types
            LOOKUP.put("SHORT", INTEGER);
            LOOKUP.put("BYTE", INTEGER);

            // add also the types with short names
            LOOKUP.put("I", INTEGER);
            LOOKUP.put("L", LONG);
            LOOKUP.put("UL", UNSIGNED_LONG);
            LOOKUP.put("D", DOUBLE);
            LOOKUP.put("K", KEYWORD);
            LOOKUP.put("S", KEYWORD);
            LOOKUP.put("STRING", KEYWORD);
            LOOKUP.put("N", NULL);
            LOOKUP.put("DATE", DATETIME);
            LOOKUP.put("DT", DATETIME);
            LOOKUP.put("V", VERSION);
        }

        private final Function<String, Object> converter;
        private final Class<?> clazz;

        Type(Function<String, Object> converter, Class<?> clazz) {
            this.converter = converter;
            this.clazz = clazz;
        }

        public static Type asType(String name) {
            return LOOKUP.get(name.toUpperCase(Locale.ROOT));
        }

        public static Type asType(ElementType elementType) {
            return switch (elementType) {
                case INT -> INTEGER;
                case LONG -> LONG;
                case DOUBLE -> DOUBLE;
                case NULL -> NULL;
                case BYTES_REF -> KEYWORD;
                case BOOLEAN -> BOOLEAN;
                case DOC -> throw new IllegalArgumentException("can't assert on doc blocks");
                case UNKNOWN -> throw new IllegalArgumentException("Unknown block types cannot be handled");
            };
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
    }

    record ActualResults(
        List<String> columnNames,
        List<Type> columnTypes,
        List<String> dataTypes,
        List<Page> pages,
        Map<String, List<String>> responseHeaders
    ) {
        List<List<Object>> values() {
            return EsqlQueryResponse.pagesToValues(dataTypes(), pages);
        }
    }

    static void logMetaData(List<String> actualColumnNames, List<Type> actualColumnTypes, Logger logger) {
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

    static void logData(List<List<Object>> values, Logger logger) {
        for (List<Object> list : values) {
            logger.info(rowAsString(list));
        }
    }

    private static String rowAsString(List<Object> list) {
        StringBuilder sb = new StringBuilder();
        StringBuilder column = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            column.setLength(0);
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append(trimOrPad(column.append(list.get(i))));
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
