/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class DelimitedTextStructureFinder implements TextStructureFinder {

    static final int MAX_EXCLUDE_LINES_PATTERN_LENGTH = 1000;
    static final String REGEX_NEEDS_ESCAPE_PATTERN = "([\\\\|()\\[\\]{}^$.+*?])";
    private static final int MAX_LEVENSHTEIN_COMPARISONS = 100;
    private static final int LONG_FIELD_THRESHOLD = 100;
    private static final int LOW_CARDINALITY_MAX_SIZE = 5;
    private static final int LOW_CARDINALITY_MIN_RATIO = 3;
    private final List<String> sampleMessages;
    private final TextStructure structure;

    static DelimitedTextStructureFinder makeDelimitedTextStructureFinder(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        CsvPreference csvPreference,
        boolean trimFields,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException {

        Tuple<List<List<String>>, List<Integer>> parsed = readRows(sample, csvPreference, timeoutChecker);
        List<List<String>> rows = parsed.v1();
        List<Integer> lineNumbers = parsed.v2();

        // Even if the column names are overridden we need to know if there's a
        // header in the text, as it affects which rows are considered records
        Tuple<Boolean, String[]> headerInfo = findHeaderFromSample(explanation, rows, overrides);
        boolean isHeaderInText = headerInfo.v1();
        String[] header = headerInfo.v2();

        String[] columnNames;
        List<String> overriddenColumnNames = overrides.getColumnNames();
        if (overriddenColumnNames != null) {
            if (overriddenColumnNames.size() != header.length) {
                throw new IllegalArgumentException(
                    "["
                        + overriddenColumnNames.size()
                        + "] column names were specified ["
                        + String.join(",", overriddenColumnNames)
                        + "] but there are ["
                        + header.length
                        + "] columns in the sample"
                );
            }
            columnNames = overriddenColumnNames.toArray(new String[0]);
        } else {
            // The column names are the header names but with dots replaced with underscores and blanks named column1, column2, etc.
            columnNames = new String[header.length];
            for (int i = 0; i < header.length; ++i) {
                assert header[i] != null;
                String rawHeader = trimFields ? header[i].trim() : header[i];
                columnNames[i] = rawHeader.isEmpty() ? "column" + (i + 1) : rawHeader.replace('.', '_');
            }
        }

        int maxLinesPerMessage = 1;
        List<String> sampleLines = Arrays.asList(sample.split("\n"));
        List<String> sampleMessages = new ArrayList<>();
        List<Map<String, ?>> sampleRecords = new ArrayList<>();
        int prevMessageEndLineNumber = isHeaderInText ? lineNumbers.get(0) : 0; // This is an exclusive end
        for (int index = isHeaderInText ? 1 : 0; index < rows.size(); ++index) {
            List<String> row = rows.get(index);
            int lineNumber = lineNumbers.get(index);
            // Indicates an illformatted row. We allow a certain number of these
            if (row.size() != columnNames.length) {
                prevMessageEndLineNumber = lineNumber;
                continue;
            }
            Map<String, String> sampleRecord = new LinkedHashMap<>();
            Util.filterListToMap(
                sampleRecord,
                columnNames,
                trimFields ? row.stream().map(field -> (field == null) ? null : field.trim()).collect(Collectors.toList()) : row
            );
            sampleRecords.add(sampleRecord);
            sampleMessages.add(String.join("\n", sampleLines.subList(prevMessageEndLineNumber, lineNumber)));
            maxLinesPerMessage = Math.max(maxLinesPerMessage, lineNumber - prevMessageEndLineNumber);
            prevMessageEndLineNumber = lineNumber;
        }

        String preamble = String.join("\n", sampleLines.subList(0, lineNumbers.get(1))) + "\n";

        // null to allow GC before timestamp search
        sampleLines = null;

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, sampleRecords, timeoutChecker);

        SortedMap<String, Object> fieldMappings = mappingsAndFieldStats.v1();

        List<String> columnNamesList = Arrays.asList(columnNames);
        char delimiter = (char) csvPreference.getDelimiterChar();
        char quoteChar = csvPreference.getQuoteChar();

        Map<String, Object> csvProcessorSettings = makeCsvProcessorSettings("message", columnNamesList, delimiter, quoteChar, trimFields);

        TextStructure.Builder structureBuilder = new TextStructure.Builder(TextStructure.Format.DELIMITED).setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble)
            .setNumLinesAnalyzed(lineNumbers.get(lineNumbers.size() - 1))
            .setNumMessagesAnalyzed(sampleRecords.size())
            .setHasHeaderRow(isHeaderInText)
            .setDelimiter(delimiter)
            .setQuote(quoteChar)
            .setColumnNames(columnNamesList);

        String quote = String.valueOf(quoteChar);
        String quotePattern = quote.replaceAll(REGEX_NEEDS_ESCAPE_PATTERN, "\\\\$1");
        String optQuotePattern = quotePattern + "?";
        String delimiterPattern = (delimiter == '\t') ? "\\t" : String.valueOf(delimiter).replaceAll(REGEX_NEEDS_ESCAPE_PATTERN, "\\\\$1");
        if (isHeaderInText) {
            structureBuilder.setExcludeLinesPattern(makeExcludeLinesPattern(header, quote, optQuotePattern, delimiterPattern));
        }

        if (trimFields) {
            structureBuilder.setShouldTrimFields(true);
        }

        Tuple<String, TimestampFormatFinder> timeField = TextStructureUtils.guessTimestampField(
            explanation,
            sampleRecords,
            overrides,
            timeoutChecker
        );
        if (timeField != null) {

            boolean needClientTimeZone = timeField.v2().hasTimezoneDependentParsing();

            structureBuilder.setTimestampField(timeField.v1())
                .setJodaTimestampFormats(timeField.v2().getJodaTimestampFormats())
                .setJavaTimestampFormats(timeField.v2().getJavaTimestampFormats())
                .setNeedClientTimezone(needClientTimeZone)
                .setEcsCompatibility(overrides.getEcsCompatibility())
                .setIngestPipeline(
                    TextStructureUtils.makeIngestPipelineDefinition(
                        null,
                        Collections.emptyMap(),
                        csvProcessorSettings,
                        fieldMappings,
                        timeField.v1(),
                        timeField.v2().getJavaTimestampFormats(),
                        needClientTimeZone,
                        timeField.v2().needNanosecondPrecision(),
                        overrides.getEcsCompatibility()
                    )
                )
                .setMultilineStartPattern(
                    makeMultilineStartPattern(
                        explanation,
                        columnNamesList,
                        maxLinesPerMessage,
                        delimiter,
                        delimiterPattern,
                        quotePattern,
                        fieldMappings,
                        sampleRecords,
                        timeField.v1(),
                        timeField.v2(),
                        timeoutChecker
                    )
                );

            fieldMappings.put(TextStructureUtils.DEFAULT_TIMESTAMP_FIELD, timeField.v2().getEsDateMappingTypeWithoutFormat());
        } else {
            structureBuilder.setIngestPipeline(
                TextStructureUtils.makeIngestPipelineDefinition(
                    null,
                    Collections.emptyMap(),
                    csvProcessorSettings,
                    fieldMappings,
                    null,
                    null,
                    false,
                    false,
                    null
                )
            );
            structureBuilder.setMultilineStartPattern(
                makeMultilineStartPattern(
                    explanation,
                    columnNamesList,
                    maxLinesPerMessage,
                    delimiter,
                    delimiterPattern,
                    quotePattern,
                    fieldMappings,
                    sampleRecords,
                    null,
                    null,
                    timeoutChecker
                )
            );
        }

        if (mappingsAndFieldStats.v2() != null) {
            structureBuilder.setFieldStats(mappingsAndFieldStats.v2());
        }

        TextStructure structure = structureBuilder.setMappings(
            Collections.singletonMap(TextStructureUtils.MAPPING_PROPERTIES_SETTING, fieldMappings)
        ).setExplanation(explanation).build();

        return new DelimitedTextStructureFinder(sampleMessages, structure);
    }

    private DelimitedTextStructureFinder(List<String> sampleMessages, TextStructure structure) {
        this.sampleMessages = Collections.unmodifiableList(sampleMessages);
        this.structure = structure;
    }

    @Override
    public List<String> getSampleMessages() {
        return sampleMessages;
    }

    @Override
    public TextStructure getStructure() {
        return structure;
    }

    static Tuple<List<List<String>>, List<Integer>> readRows(String sample, CsvPreference csvPreference, TimeoutChecker timeoutChecker)
        throws IOException {

        int fieldsInFirstRow = -1;

        List<List<String>> rows = new ArrayList<>();
        List<Integer> lineNumbers = new ArrayList<>();

        try (CsvListReader csvReader = new CsvListReader(new StringReader(sample), csvPreference)) {

            try {
                List<String> row;
                while ((row = csvReader.read()) != null) {
                    if (fieldsInFirstRow < 0) {
                        fieldsInFirstRow = row.size();
                    } else {
                        // Tolerate extra columns if and only if they're empty
                        while (row.size() > fieldsInFirstRow && row.get(row.size() - 1) == null) {
                            row.remove(row.size() - 1);
                        }
                    }
                    rows.add(row);
                    timeoutChecker.check("delimited record parsing");
                    lineNumbers.add(csvReader.getLineNumber());
                }
            } catch (SuperCsvException e) {
                // Tolerate an incomplete last row
                if (notUnexpectedEndOfFile(e)) {
                    throw e;
                }
            }
        }

        assert rows.isEmpty() == false;
        assert lineNumbers.size() == rows.size();

        if (rows.get(0).size() != rows.get(rows.size() - 1).size()) {
            rows.remove(rows.size() - 1);
            lineNumbers.remove(lineNumbers.size() - 1);
        }

        // This should have been enforced by canCreateFromSample()
        assert rows.size() > 1;

        return new Tuple<>(rows, lineNumbers);
    }

    static Tuple<Boolean, String[]> findHeaderFromSample(
        List<String> explanation,
        List<List<String>> rows,
        TextStructureOverrides overrides
    ) {

        assert rows.isEmpty() == false;

        List<String> overriddenColumnNames = overrides.getColumnNames();
        List<String> firstRow = rows.get(0);

        boolean isHeaderInText = true;
        if (overrides.getHasHeaderRow() != null) {
            isHeaderInText = overrides.getHasHeaderRow();
            if (isHeaderInText && overriddenColumnNames == null) {
                String duplicateValue = findDuplicateNonEmptyValues(firstRow);
                if (duplicateValue != null) {
                    throw new IllegalArgumentException(
                        "Sample specified to contain a header row, "
                            + "but the first row contains duplicate values: ["
                            + duplicateValue
                            + "]"
                    );
                }
            }
            explanation.add("Sample specified to " + (isHeaderInText ? "contain" : "not contain") + " a header row");
        } else {
            if (findDuplicateNonEmptyValues(firstRow) != null) {
                isHeaderInText = false;
                explanation.add("First row contains duplicate values, so assuming it's not a header");
            } else {
                if (rows.size() < 3) {
                    explanation.add("Too little data to accurately assess whether header is in sample - guessing it is");
                } else {
                    isHeaderInText = isFirstRowUnusual(explanation, rows);
                }
            }
        }

        String[] header;
        if (isHeaderInText) {
            // SuperCSV will put nulls in the header if any columns don't have names, but empty strings are better for us
            header = firstRow.stream().map(field -> (field == null) ? "" : field).toArray(String[]::new);
        } else {
            header = new String[firstRow.size()];
            Arrays.fill(header, "");
        }

        return new Tuple<>(isHeaderInText, header);
    }

    static String findDuplicateNonEmptyValues(List<String> row) {

        HashSet<String> values = new HashSet<>();

        for (String value : row) {
            if (value != null && value.isEmpty() == false && values.add(value) == false) {
                return value;
            }
        }

        return null;
    }

    private static boolean isFirstRowUnusual(List<String> explanation, List<List<String>> rows) {

        assert rows.size() >= 3;

        List<String> firstRow = rows.get(0);
        String firstRowStr = firstRow.stream().map(field -> (field == null) ? "" : field).collect(Collectors.joining(""));
        List<List<String>> otherRows = rows.subList(1, rows.size());
        List<String> otherRowStrs = new ArrayList<>();
        for (List<String> row : otherRows) {
            otherRowStrs.add(row.stream().map(str -> (str == null) ? "" : str).collect(Collectors.joining("")));
        }

        // Check lengths

        double firstRowLength = firstRowStr.length();
        DoubleSummaryStatistics otherRowStats = otherRowStrs.stream()
            .mapToDouble(otherRow -> (double) otherRow.length())
            .collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);

        double otherLengthRange = otherRowStats.getMax() - otherRowStats.getMin();
        if (firstRowLength < otherRowStats.getMin() - otherLengthRange / 10.0
            || firstRowLength > otherRowStats.getMax() + otherLengthRange / 10.0) {
            explanation.add(
                "First row is unusual based on length test: [" + firstRowLength + "] and [" + toNiceString(otherRowStats) + "]"
            );
            return true;
        }

        explanation.add(
            "First row is not unusual based on length test: [" + firstRowLength + "] and [" + toNiceString(otherRowStats) + "]"
        );

        // Check edit distances between short fields

        BitSet shortFieldMask = makeShortFieldMask(rows, LONG_FIELD_THRESHOLD);

        // The reason that only short fields are included is that sometimes
        // there are "message" fields that are much longer than the other
        // fields, vary enormously between rows, and skew the comparison.
        DoubleSummaryStatistics firstRowStats = otherRows.stream()
            .limit(MAX_LEVENSHTEIN_COMPARISONS)
            .mapToDouble(otherRow -> (double) levenshteinFieldwiseCompareRows(firstRow, otherRow, shortFieldMask))
            .collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);

        otherRowStats = new DoubleSummaryStatistics();
        int numComparisons = 0;
        int proportion = otherRowStrs.size() / MAX_LEVENSHTEIN_COMPARISONS;
        int innerIncrement = 1 + proportion * proportion;
        Random random = new Random(firstRow.hashCode());
        for (int i = 0; numComparisons < MAX_LEVENSHTEIN_COMPARISONS && i < otherRowStrs.size(); ++i) {
            for (int j = i + 1 + random.nextInt(innerIncrement); numComparisons < MAX_LEVENSHTEIN_COMPARISONS
                && j < otherRowStrs.size(); j += innerIncrement) {
                otherRowStats.accept(levenshteinFieldwiseCompareRows(otherRows.get(i), otherRows.get(j), shortFieldMask));
                ++numComparisons;
            }
        }

        if (firstRowStats.getAverage() > otherRowStats.getAverage() * 1.2) {
            explanation.add(
                "First row is unusual based on Levenshtein test ["
                    + toNiceString(firstRowStats)
                    + "] and ["
                    + toNiceString(otherRowStats)
                    + "]"
            );
            return true;
        }

        explanation.add(
            "First row is not unusual based on Levenshtein test ["
                + toNiceString(firstRowStats)
                + "] and ["
                + toNiceString(otherRowStats)
                + "]"
        );

        return false;
    }

    private static String toNiceString(DoubleSummaryStatistics stats) {
        return String.format(
            Locale.ROOT,
            "count=%d, min=%f, average=%f, max=%f",
            stats.getCount(),
            stats.getMin(),
            stats.getAverage(),
            stats.getMax()
        );
    }

    /**
     * Make a mask whose bits are set when the corresponding field in every supplied
     * row is short, and unset if the corresponding field in any supplied row is long.
     */
    static BitSet makeShortFieldMask(List<List<String>> rows, int longFieldThreshold) {

        assert rows.isEmpty() == false;

        BitSet shortFieldMask = new BitSet();

        int maxLength = rows.stream().map(List::size).max(Integer::compareTo).get();
        for (int index = 0; index < maxLength; ++index) {
            final int i = index;
            shortFieldMask.set(
                i,
                rows.stream().allMatch(row -> i >= row.size() || row.get(i) == null || row.get(i).length() < longFieldThreshold)
            );
        }

        return shortFieldMask;
    }

    /**
     * Sum of the Levenshtein distances between corresponding elements
     * in the two supplied lists.
     */
    static int levenshteinFieldwiseCompareRows(List<String> firstRow, List<String> secondRow) {

        int largestSize = Math.max(firstRow.size(), secondRow.size());
        if (largestSize < 1) {
            return 0;
        }

        BitSet allFields = new BitSet();
        allFields.set(0, largestSize);

        return levenshteinFieldwiseCompareRows(firstRow, secondRow, allFields);
    }

    /**
     * Sum of the Levenshtein distances between corresponding elements
     * in the two supplied lists where the corresponding bit in the
     * supplied bit mask is set.
     */
    static int levenshteinFieldwiseCompareRows(List<String> firstRow, List<String> secondRow, BitSet fieldMask) {

        int result = 0;

        for (int index = fieldMask.nextSetBit(0); index >= 0; index = fieldMask.nextSetBit(index + 1)) {
            result += levenshteinDistance(
                (index < firstRow.size()) ? firstRow.get(index) : "",
                (index < secondRow.size()) ? secondRow.get(index) : ""
            );
        }

        return result;
    }

    /**
     * This method implements the simple algorithm for calculating Levenshtein distance.
     */
    static int levenshteinDistance(String first, String second) {

        // There are some examples with pretty pictures of the matrix on Wikipedia here:
        // http://en.wikipedia.org/wiki/Levenshtein_distance

        int firstLen = (first == null) ? 0 : first.length();
        int secondLen = (second == null) ? 0 : second.length();
        if (firstLen == 0) {
            return secondLen;
        }
        if (secondLen == 0) {
            return firstLen;
        }

        int[] currentCol = new int[secondLen + 1];
        int[] prevCol = new int[secondLen + 1];

        // Populate the left column
        for (int down = 0; down <= secondLen; ++down) {
            currentCol[down] = down;
        }

        // Calculate the other entries in the matrix
        for (int across = 1; across <= firstLen; ++across) {
            int[] tmp = prevCol;
            prevCol = currentCol;
            // We could allocate a new array for currentCol here, but it's more efficient to reuse the one that's now redundant
            currentCol = tmp;

            currentCol[0] = across;

            for (int down = 1; down <= secondLen; ++down) {

                // Do the strings differ at the point we've reached?
                if (first.charAt(across - 1) == second.charAt(down - 1)) {

                    // No, they're the same => no extra cost
                    currentCol[down] = prevCol[down - 1];
                } else {
                    // Yes, they differ, so there are 3 options:

                    // 1) Deletion => cell to the left's value plus 1
                    int option1 = prevCol[down];

                    // 2) Insertion => cell above's value plus 1
                    int option2 = currentCol[down - 1];

                    // 3) Substitution => cell above left's value plus 1
                    int option3 = prevCol[down - 1];

                    // Take the cheapest option of the 3
                    currentCol[down] = Math.min(Math.min(option1, option2), option3) + 1;
                }
            }
        }

        // Result is the value in the bottom right hand corner of the matrix
        return currentCol[secondLen];
    }

    static boolean lineHasUnescapedQuote(String line, CsvPreference csvPreference) {
        char quote = csvPreference.getQuoteChar();
        String lineWithEscapedQuotesRemoved = line.replace(String.valueOf(quote) + quote, "");
        for (int index = 1; index < lineWithEscapedQuotesRemoved.length() - 1; ++index) {
            if (lineWithEscapedQuotesRemoved.charAt(index) == quote
                && lineWithEscapedQuotesRemoved.codePointAt(index - 1) != csvPreference.getDelimiterChar()
                && lineWithEscapedQuotesRemoved.codePointAt(index + 1) != csvPreference.getDelimiterChar()) {
                return true;
            }
        }
        return false;
    }

    static boolean canCreateFromSample(
        List<String> explanation,
        String sample,
        int minFieldsPerRow,
        CsvPreference csvPreference,
        String formatName,
        double allowedFractionOfBadLines
    ) {

        // Logstash's CSV parser won't tolerate fields where just part of the
        // value is quoted, whereas SuperCSV will, hence this extra check
        String[] sampleLines = sample.split("\n");
        for (String sampleLine : sampleLines) {
            if (lineHasUnescapedQuote(sampleLine, csvPreference)) {
                explanation.add(
                    "Not "
                        + formatName
                        + " because a line has an unescaped quote that is not at the beginning or end of a field: ["
                        + sampleLine
                        + "]"
                );
                return false;
            }
        }

        int numberOfLinesInSample = sampleLines.length;
        try (CsvListReader csvReader = new CsvListReader(new StringReader(sample), csvPreference)) {

            int fieldsInFirstRow = -1;
            int fieldsInLastRow = -1;

            List<Integer> illFormattedRows = new ArrayList<>();
            int numberOfRows = 0;
            try {
                List<String> row;
                while ((row = csvReader.read()) != null) {

                    int fieldsInThisRow = row.size();
                    ++numberOfRows;
                    if (fieldsInFirstRow < 0) {
                        fieldsInFirstRow = fieldsInThisRow;
                        if (fieldsInFirstRow < minFieldsPerRow) {
                            explanation.add(
                                "Not "
                                    + formatName
                                    + " because the first row has fewer than ["
                                    + minFieldsPerRow
                                    + "] fields: ["
                                    + fieldsInFirstRow
                                    + "]"
                            );
                            return false;
                        }
                        fieldsInLastRow = fieldsInFirstRow;
                        continue;
                    }

                    // Tolerate extra columns if and only if they're empty
                    while (fieldsInThisRow > fieldsInFirstRow && row.get(fieldsInThisRow - 1) == null) {
                        --fieldsInThisRow;
                    }

                    // TODO: might be good one day to gather a distribution of the most common field counts
                    // But, this would require iterating (or at least sampling) all the lines.
                    if (fieldsInThisRow != fieldsInFirstRow) {
                        illFormattedRows.add(numberOfRows - 1);
                        // This calculation is complicated by the possibility of multi-lined CSV columns
                        // `getLineNumber` is a current count of lines, regardless of row count, so
                        // this formula is just an approximation, but gets more accurate the further
                        // through the sample you are.
                        double totalNumberOfRows = (numberOfRows + numberOfLinesInSample - csvReader.getLineNumber());
                        // We should only allow a certain percentage of ill formatted rows
                        // as it may have and down stream effects
                        if (illFormattedRows.size() > Math.ceil(allowedFractionOfBadLines * totalNumberOfRows)) {
                            explanation.add(
                                format(
                                    "Not %s because %s or more rows did not have the same number of fields "
                                        + "as the first row (%s). Bad rows %s",
                                    formatName,
                                    illFormattedRows.size(),
                                    fieldsInFirstRow,
                                    illFormattedRows
                                )
                            );
                            return false;
                        }
                        continue;
                    }

                    fieldsInLastRow = fieldsInThisRow;
                }

                if (fieldsInLastRow > fieldsInFirstRow) {
                    explanation.add(
                        "Not "
                            + formatName
                            + " because last row has more fields than first row: ["
                            + fieldsInFirstRow
                            + "] and ["
                            + fieldsInLastRow
                            + "]"
                    );
                    return false;
                }
                if (fieldsInLastRow < fieldsInFirstRow) {
                    --numberOfRows;
                }
            } catch (SuperCsvException e) {
                // Tolerate an incomplete last row
                if (notUnexpectedEndOfFile(e)) {
                    explanation.add("Not " + formatName + " because there was a parsing exception: [" + e.getMessage() + "]");
                    return false;
                }
            }
            if (numberOfRows <= 1) {
                explanation.add("Not " + formatName + " because fewer than 2 complete records in sample: [" + numberOfRows + "]");
                return false;
            }
            explanation.add("Deciding sample is " + formatName);
            return true;

        } catch (IOException e) {
            explanation.add("Not " + formatName + " because there was a parsing exception: [" + e.getMessage() + "]");
            return false;
        }
    }

    private static boolean notUnexpectedEndOfFile(SuperCsvException e) {
        return e.getMessage().startsWith("unexpected end of file while reading quoted column") == false;
    }

    static Map<String, Object> makeCsvProcessorSettings(String field, List<String> targetFields, char separator, char quote, boolean trim) {

        Map<String, Object> csvProcessorSettings = new LinkedHashMap<>();
        csvProcessorSettings.put("field", field);
        csvProcessorSettings.put("target_fields", Collections.unmodifiableList(targetFields));
        if (separator != ',') {
            // The value must be String, not Character, as XContent only works with String
            csvProcessorSettings.put("separator", String.valueOf(separator));
        }
        if (quote != '"') {
            // The value must be String, not Character, as XContent only works with String
            csvProcessorSettings.put("quote", String.valueOf(quote));
        }
        csvProcessorSettings.put("ignore_missing", false);
        if (trim) {
            csvProcessorSettings.put("trim", true);
        }
        return Collections.unmodifiableMap(csvProcessorSettings);
    }

    /**
     * The multi-line start pattern is based on the first field in the line that is boolean, numeric
     * or the detected timestamp, and consists of a pattern matching that field, preceded by wildcards
     * to match any prior fields and to match the delimiters in between them.
     *
     * This is based on the observation that a boolean, numeric or timestamp field will not contain a
     * newline.
     *
     * The approach works best when the chosen field is early in each record, ideally the very first
     * field.  It doesn't work when fields prior to the chosen field contain newlines in some of the
     * records.
     */
    static String makeMultilineStartPattern(
        List<String> explanation,
        List<String> columnNames,
        int maxLinesPerMessage,
        char delimiter,
        String delimiterPattern,
        String quotePattern,
        Map<String, Object> fieldMappings,
        List<Map<String, ?>> sampleRecords,
        String timeFieldName,
        TimestampFormatFinder timeFieldFormat,
        TimeoutChecker timeoutChecker
    ) {

        assert columnNames.isEmpty() == false;
        assert maxLinesPerMessage > 0;
        assert (timeFieldName == null) == (timeFieldFormat == null);

        // This is the easy case: text where there are no multi-line fields
        if (maxLinesPerMessage == 1) {
            explanation.add("Not creating a multi-line start pattern as no sampled message spanned multiple lines");
            return null;
        }

        StringBuilder builder = new StringBuilder("^");
        // Look for a field early in the line that cannot be a multi-line field based on the type we've determined for
        // it, and create a pattern that matches this field with the appropriate number of delimiters before it.
        // There is no point doing this for the last field on the line, so this is why the loop excludes the last column.
        for (String columnName : columnNames.subList(0, columnNames.size() - 1)) {
            if (columnName.equals(timeFieldName)) {
                builder.append(quotePattern).append("?");
                String simpleTimePattern = timeFieldFormat.getSimplePattern().pattern();
                builder.append(simpleTimePattern.startsWith("\\b") ? simpleTimePattern.substring(2) : simpleTimePattern);
                explanation.add("Created a multi-line start pattern based on timestamp column [" + columnName + "]");
                return builder.toString();
            }
            Object columnMapping = fieldMappings.get(columnName);
            if (columnMapping instanceof Map) {
                String type = (String) ((Map<?, ?>) columnMapping).get(TextStructureUtils.MAPPING_TYPE_SETTING);
                if (type != null) {
                    String columnPattern = switch (type) {
                        case "boolean" -> "(?:true|false)";
                        case "byte", "short", "integer", "long" -> "[+-]?\\d+";
                        case "half_float", "float", "double" -> "[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[eE][+-]?\\d+)?";
                        case "keyword" -> findLowCardinalityKeywordPattern(columnName, sampleRecords, timeoutChecker);
                        default -> null;
                    };
                    if (columnPattern != null) {
                        builder.append("(?:")
                            .append(columnPattern)
                            .append("|")
                            .append(quotePattern)
                            .append(columnPattern)
                            .append(quotePattern)
                            .append(")")
                            .append(delimiterPattern);
                        explanation.add("Created a multi-line start pattern based on [" + type + "] column [" + columnName + "]");
                        return builder.toString();
                    }
                }
            }
            // We need to be strict about how many delimiters precede the chosen field,
            // so if it's not the first then we cannot tolerate the preceding fields
            // containing the delimiter. Additionally, there's no point choosing a field
            // after a field that sometimes contains line breaks to identify the first
            // line.
            if (columnValueContainsDelimiterOrLineBreak(columnName, delimiter, sampleRecords, timeoutChecker)) {
                throw new IllegalArgumentException(
                    "Cannot create a multi-line start pattern. "
                        + "No suitable column to match exists before the first column whose values contain line breaks or delimiters ["
                        + columnName
                        + "]. If the timestamp format was not identified correctly adding an override for this may help."
                );
            }
            builder.append("[^");
            // Within a negated character class we don't want to escape special regex
            // characters like dot, hence shouldn't use the pre-built pattern
            if (delimiter == '\t') {
                builder.append("\\t");
            } else {
                builder.append(delimiter);
            }
            builder.append("]*?").append(delimiterPattern);
        }
        // TODO: if this happens a lot then we should try looking for the a multi-line END pattern instead of a start pattern.
        // But this would require changing the find_structure response, and the file upload UI, and would make creating Filebeat
        // configs from the find_structure response more complex, so let's wait to see if there's significant demand.
        explanation.add("Failed to create a suitable multi-line start pattern");
        return null;
    }

    /**
     * @return <code>true</code> if the value of the field {@code columnName} in any record in the {@code sampleRecords}
     *         contains the {@code delimiter} or a line break.
     */
    static boolean columnValueContainsDelimiterOrLineBreak(
        String columnName,
        char delimiter,
        List<Map<String, ?>> sampleRecords,
        TimeoutChecker timeoutChecker
    ) {
        for (Map<String, ?> sampleRecord : sampleRecords) {
            timeoutChecker.check("delimiter search in multi-line start pattern determination");
            Object value = sampleRecord.get(columnName);
            if (value != null) {
                String str = value.toString();
                if (str.indexOf(delimiter) >= 0 || str.indexOf('\n') >= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Try to find a regular expression that will match any of the values of a keyword field, providing:
     * 1. There are only a small number of distinct values of that keyword field
     * 2. The number of sampled records is several times bigger than the number of distinct values
     * 3. None of the values is empty or contains a line break
     * 4. None of the values matches the last line of a value of some other field in the sampled records
     * @return A regular expression that will match the small number of distinct values of the keyword field, or
     *         <code>null</code> if a suitable regular expression could not be found.
     */
    static String findLowCardinalityKeywordPattern(String columnName, List<Map<String, ?>> sampleRecords, TimeoutChecker timeoutChecker) {

        int maxCardinality = Math.min(LOW_CARDINALITY_MAX_SIZE, sampleRecords.size() / LOW_CARDINALITY_MIN_RATIO);

        // Find the distinct values of the column, aborting if there are too many or if any contain newlines.
        Set<String> values = new HashSet<>();
        for (Map<String, ?> sampleRecord : sampleRecords) {
            Object value = sampleRecord.get(columnName);
            if (value == null) {
                return null;
            }
            String str = value.toString();
            if (str.isEmpty() || str.indexOf('\n') >= 0) {
                return null;
            }
            values.add(str);
            if (values.size() > maxCardinality) {
                return null;
            }
        }

        // Check that none of the values exist in other columns.
        // In the case of field values that span multiple lines, it's the part after the last newline that matters.
        for (Map<String, ?> sampleRecord : sampleRecords) {
            timeoutChecker.check("keyword-based multi-line start pattern determination");
            if (sampleRecord.entrySet()
                .stream()
                .anyMatch(entry -> entry.getKey().equals(columnName) == false && containsLastLine(values, entry.getValue()))) {
                return null;
            }
        }

        return values.stream()
            .map(value -> value.replaceAll(REGEX_NEEDS_ESCAPE_PATTERN, "\\\\$1"))
            .sorted()
            .collect(Collectors.joining("|", "(?:", ")"));
    }

    /**
     * @param set A set of strings.
     * @param obj An object whose string representation may or may not contain line breaks.
     * @return true if {@code set} contains the last line of {@code str} (i.e. the whole of {@code str} if it has no line breaks).
     */
    static boolean containsLastLine(Set<String> set, Object obj) {
        if (obj == null) {
            return false;
        }
        String str = obj.toString();
        int lastNewline = str.lastIndexOf('\n');
        if (lastNewline >= 0) {
            return set.contains(str.substring(lastNewline + 1));
        } else {
            return set.contains(str);
        }
    }

    /**
     * Make a regular expression that Filebeat can use to ignore the header line of the delimited file.
     * (Such lines may be observed multiple times if multiple delimited files are concatenated.)
     *
     * This pattern consists of a pattern that matches the literal column names, optionally quoted and
     * separated by the delimiter.
     *
     * In the event that the column names are long and/or numerous only the first few are included.
     * These ought to be enough to reliably distinguish the header line from data lines.
     */
    static String makeExcludeLinesPattern(String[] header, String quote, String optQuotePattern, String delimiterPattern) {
        String twoQuotes = quote + quote;
        StringBuilder excludeLinesPattern = new StringBuilder("^");
        boolean isFirst = true;
        int maxLengthOfFields = MAX_EXCLUDE_LINES_PATTERN_LENGTH - delimiterPattern.length() - 2; // 2 is length of ".*"
        for (String column : header) {
            String columnPattern = optQuotePattern + column.replace(quote, twoQuotes).replaceAll(REGEX_NEEDS_ESCAPE_PATTERN, "\\\\$1")
                + optQuotePattern;
            if (isFirst) {
                // Always append the pattern for the first column, even if it exceeds the limit
                excludeLinesPattern.append(columnPattern);
                isFirst = false;
            } else {
                if (excludeLinesPattern.length() + columnPattern.length() > maxLengthOfFields) {
                    excludeLinesPattern.append(".*");
                    break;
                }
                excludeLinesPattern.append(delimiterPattern).append(columnPattern);
            }
        }
        return excludeLinesPattern.toString();
    }
}
