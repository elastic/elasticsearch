/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SeparatedValuesLogFileStructureFinder extends AbstractStructuredLogFileStructureFinder implements LogFileStructureFinder {

    private static final int MAX_LEVENSHTEIN_COMPARISONS = 100;

    private static final String FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String SEPARATOR_TEMPLATE = "    separator => \"%c\"\n";
    private static final String LOGSTASH_CONVERSIONS_TEMPLATE = "    convert => {\n" +
        "%s" +
        "    }\n";
    private static final String LOGSTASH_STRIP_FILTER_TEMPLATE = "  mutate {\n" +
        "    strip => [ %s ]\n" +
        "  }\n";
    private static final String LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  csv {\n" +
        "%s" +
        "    columns => [ %s ]\n" +
        "%s" +
        "    remove_field => [ \"message\" ]\n" +
        "  }\n" +
        "%s" +
        "%s" +
        "}\n" +
        "\n" +
        "output {\n" +
        "  elasticsearch {\n" +
        "    hosts => '%s'\n" +
        "    manage_template => false\n" +
        "    index => \"%%{[@metadata][beat]}-%%{[@metadata][version]}-%%{+YYYY.MM.dd}\"\n" +
        "  }\n" +
        "}\n";
    private static final String LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
        "%s" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  mutate {\n" +
        "    rename => {\n" +
        "      \"path\" => \"source\"\n" +
        "    }\n" +
        "  }\n" +
        "  csv {\n" +
        "%s" +
        "    columns => [ %s ]\n" +
        "%s" +
        "%s" +
        "    remove_field => [ \"message\" ]\n" +
        "  }\n" +
        "%s" +
        "%s" +
        "}\n" +
        "\n" +
        "output {\n" +
        "  elasticsearch {\n" +
        "    hosts => '%s'\n" +
        "    manage_template => false\n" +
        "    index => \"%s\"\n" +
        "    document_type => \"_doc\"\n" +
        "  }\n" +
        "}\n";

    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;

    SeparatedValuesLogFileStructureFinder(Terminal terminal, String sampleFileName, String indexName, String typeName,
                                          String elasticsearchHost, String logstashHost, String logstashFileTimezone, String sample,
                                          String charsetName, Boolean hasByteOrderMarker, CsvPreference csvPreference, boolean trimFields)
        throws IOException, UserException {
        super(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost, logstashFileTimezone, charsetName,
            hasByteOrderMarker);

        Tuple<List<List<String>>, List<Integer>> parsed = readRows(sample, csvPreference);
        List<List<String>> rows = parsed.v1();
        List<Integer> lineNumbers = parsed.v2();

        Tuple<Boolean, String[]> headerInfo = findHeaderFromSample(terminal, rows);
        boolean isCsvHeaderInFile = headerInfo.v1();
        String[] csvHeader = headerInfo.v2();
        String[] csvHeaderWithNamedBlanks = new String[csvHeader.length];
        for (int i = 0; i < csvHeader.length; ++i) {
            String rawHeader = csvHeader[i].isEmpty() ? "column" + (i + 1) : csvHeader[i];
            csvHeaderWithNamedBlanks[i] = trimFields ? rawHeader.trim() : rawHeader;
        }

        List<Map<String, ?>> sampleRecords = new ArrayList<>();
        for (List<String> row : isCsvHeaderInFile ? rows.subList(1, rows.size()): rows) {
            Map<String, String> sampleRecord = new HashMap<>();
            Util.filterListToMap(sampleRecord, csvHeaderWithNamedBlanks,
                trimFields ? row.stream().map(String::trim).collect(Collectors.toList()) : row);
            sampleRecords.add(sampleRecord);
        }

        String preamble = Pattern.compile("\n").splitAsStream(sample).limit(lineNumbers.get(1)).collect(Collectors.joining("\n", "", "\n"));

        char delimiter = (char) csvPreference.getDelimiterChar();
        LogFileStructure.Builder structureBuilder = new LogFileStructure.Builder(LogFileStructure.Format.fromSeparator(delimiter))
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble)
            .setNumLinesAnalyzed(lineNumbers.get(lineNumbers.size() - 1))
            .setNumMessagesAnalyzed(sampleRecords.size())
            .setHasHeaderRow(isCsvHeaderInFile)
            .setInputFields(Arrays.stream(csvHeaderWithNamedBlanks).collect(Collectors.toList()));

        if (trimFields) {
            structureBuilder.setShouldTrimFields(true);
        }

        Tuple<String, TimestampMatch> timeField = guessTimestampField(sampleRecords);
        if (timeField != null) {
            String timeLineRegex = null;
            StringBuilder builder = new StringBuilder("^");
            // We make the assumption that the timestamp will be on the first line of each record.  Therefore, if the
            // timestamp is the last column then either our assumption is wrong (and the approach will completely
            // break down) or else every record is on a single line and there's no point creating a multiline config.
            // This is why the loop excludes the last column.
            for (String column : Arrays.asList(csvHeader).subList(0, csvHeader.length - 1)) {
                if (timeField.v1().equals(column)) {
                    builder.append("\"?");
                    String simpleTimePattern = timeField.v2().simplePattern.pattern();
                    builder.append(simpleTimePattern.startsWith("\\b") ? simpleTimePattern.substring(2) : simpleTimePattern);
                    timeLineRegex = builder.toString();
                    break;
                } else {
                    builder.append(".*?");
                    if (delimiter == '\t') {
                        builder.append("\\t");
                    } else {
                        builder.append(delimiter);
                    }
                }
            }

            if (isCsvHeaderInFile) {
                structureBuilder.setExcludeLinesPattern(Arrays.stream(csvHeader)
                    .map(column -> "\"?" + column.replace("\"", "\"\"").replaceAll("([\\\\|()\\[\\]{}^$*?])", "\\\\$1") + "\"?")
                    .collect(Collectors.joining(",")));
            }

            structureBuilder.setTimestampField(timeField.v1())
                .setTimestampFormats(timeField.v2().dateFormats)
                .setNeedClientTimezone(timeField.v2().hasTimezoneDependentParsing())
                .setMultilineStartPattern(timeLineRegex);
        }

        SortedMap<String, Object> mappings = guessMappings(sampleRecords);
        mappings.put(DEFAULT_TIMESTAMP_FIELD, Collections.singletonMap(MAPPING_TYPE_SETTING, "date"));

        structure = structureBuilder
            .setMappings(mappings)
            .setExplanation(Collections.singletonList("TODO")) // TODO
            .build();
    }

    static Tuple<List<List<String>>, List<Integer>> readRows(String sample, CsvPreference csvPreference) throws IOException {

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

    static Tuple<Boolean, String[]> findHeaderFromSample(Terminal terminal, List<List<String>> rows) {

        assert rows.isEmpty() == false;

        List<String> firstRow = rows.get(0);
        boolean isCsvHeaderInFile = true;

        if (rows.size() < 3) {
            terminal.println(Verbosity.VERBOSE, "Too little data to accurately assess whether header is in sample - guessing it is");
        } else {
            isCsvHeaderInFile = isFirstRowUnusual(terminal, rows);
        }

        if (isCsvHeaderInFile) {
            // SuperCSV will put nulls in the header if any columns don't have names, but empty strings are better for us
            return new Tuple<>(true, firstRow.stream().map(field -> (field == null) ? "" : field).toArray(String[]::new));
        } else {
            return new Tuple<>(false, IntStream.rangeClosed(1, firstRow.size()).mapToObj(num -> "column" + num).toArray(String[]::new));
        }
    }

    private static boolean isFirstRowUnusual(Terminal terminal, List<List<String>> rows) {

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
        DoubleSummaryStatistics otherRowStats = otherRowStrs.stream().mapToDouble(otherRow -> (double) otherRow.length())
            .collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);

        double otherLengthRange = otherRowStats.getMax() - otherRowStats.getMin();
        if (firstRowLength < otherRowStats.getMin() - otherLengthRange / 10.0 ||
            firstRowLength > otherRowStats.getMax() + otherLengthRange / 10.0) {
            terminal.println(Verbosity.VERBOSE, "First row is unusual based on length test: [" + firstRowLength + "] and [" +
                toNiceString(otherRowStats) + "]");
            return true;
        }

        terminal.println(Verbosity.VERBOSE, "First row is not unusual based on length test: [" + firstRowLength + "] and [" +
            toNiceString(otherRowStats) + "]");

        // Check edit distances

        DoubleSummaryStatistics firstRowStats = otherRows.stream().limit(MAX_LEVENSHTEIN_COMPARISONS)
            .mapToDouble(otherRow -> (double) levenshteinFieldwiseCompareRows(firstRow, otherRow))
            .collect(DoubleSummaryStatistics::new, DoubleSummaryStatistics::accept, DoubleSummaryStatistics::combine);

        otherRowStats = new DoubleSummaryStatistics();
        int numComparisons = 0;
        int proportion = otherRowStrs.size() / MAX_LEVENSHTEIN_COMPARISONS;
        int innerIncrement = 1 + proportion * proportion;
        Random random = new Random(firstRow.hashCode());
        for (int i = 0; numComparisons < MAX_LEVENSHTEIN_COMPARISONS && i < otherRowStrs.size(); ++i) {
            for (int j = i + 1 + random.nextInt(innerIncrement); numComparisons < MAX_LEVENSHTEIN_COMPARISONS && j < otherRowStrs.size();
                 j += innerIncrement) {
                otherRowStats.accept((double) levenshteinFieldwiseCompareRows(otherRows.get(i), otherRows.get(j)));
                ++numComparisons;
            }
        }

        if (firstRowStats.getAverage() > otherRowStats.getAverage() * 1.2) {
            terminal.println(Verbosity.VERBOSE, "First row is unusual based on Levenshtein test [" + toNiceString(firstRowStats) +
                "] and [" + toNiceString(otherRowStats) + "]");
            return true;
        }

        terminal.println(Verbosity.VERBOSE, "First row is not unusual based on Levenshtein test [" + toNiceString(firstRowStats) +
            "] and [" + toNiceString(otherRowStats) + "]");

        return false;
    }

    private static String toNiceString(DoubleSummaryStatistics stats) {
        return String.format(Locale.ROOT, "count=%d, min=%f, average=%f, max=%f", stats.getCount(), stats.getMin(), stats.getAverage(),
            stats.getMax());
    }

    /**
     * Sum of the Levenshtein distances between corresponding elements
     * in the two supplied lists _excluding_ the biggest difference.
     * The reason the biggest difference is excluded is that sometimes
     * there's a "message" field that is much longer than any of the other
     * fields, varies enormously between rows, and skews the comparison.
     */
    static int levenshteinFieldwiseCompareRows(List<String> firstRow, List<String> secondRow) {

        int largestSize = Math.max(firstRow.size(), secondRow.size());
        if (largestSize <= 1) {
            return 0;
        }

        int[] distances = new int[largestSize];

        for (int index = 0; index < largestSize; ++index) {
            distances[index] = levenshteinDistance((index < firstRow.size()) ? firstRow.get(index) : "",
                (index < secondRow.size()) ? secondRow.get(index) : "");
        }

        Arrays.sort(distances);

        return IntStream.of(distances).limit(distances.length - 1).sum();
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
            if (lineWithEscapedQuotesRemoved.charAt(index) == quote &&
                lineWithEscapedQuotesRemoved.codePointAt(index - 1) != csvPreference.getDelimiterChar() &&
                lineWithEscapedQuotesRemoved.codePointAt(index + 1) != csvPreference.getDelimiterChar()) {
                return true;
            }
        }
        return false;
    }

    static boolean canCreateFromSample(Terminal terminal, String sample, int minFieldsPerRow, CsvPreference csvPreference,
                                       String formatName) {

        // Logstash's CSV parser won't tolerate fields where just part of the
        // value is quoted, whereas SuperCSV will, hence this extra check
        String[] sampleLines = sample.split("\n");
        for (String sampleLine : sampleLines) {
            if (lineHasUnescapedQuote(sampleLine, csvPreference)) {
                terminal.println(Verbosity.VERBOSE, "Not " + formatName +
                    " because a line has an unescaped quote that is not at the beginning or end of a field: [" + sampleLine + "]");
                return false;
            }
        }

        try (CsvListReader csvReader = new CsvListReader(new StringReader(sample), csvPreference)) {

            int fieldsInFirstRow = -1;
            int fieldsInLastRow = -1;

            int numberOfRows = 0;
            try {
                List<String> row;
                while ((row = csvReader.read()) != null) {

                    int fieldsInThisRow = row.size();
                    ++numberOfRows;
                    if (fieldsInFirstRow < 0) {
                        fieldsInFirstRow = fieldsInThisRow;
                        if (fieldsInFirstRow < minFieldsPerRow) {
                            terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because the first row has fewer than [" +
                                minFieldsPerRow + "] fields: [" + fieldsInFirstRow + "]");
                            return false;
                        }
                        fieldsInLastRow = fieldsInFirstRow;
                        continue;
                    }

                    // Tolerate extra columns if and only if they're empty
                    while (fieldsInThisRow > fieldsInFirstRow && row.get(fieldsInThisRow - 1) == null) {
                        --fieldsInThisRow;
                    }

                    if (fieldsInLastRow != fieldsInFirstRow) {
                        terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because row [" + (numberOfRows - 1) +
                            "] has a different number of fields to the first row: [" + fieldsInFirstRow + "] and [" +
                            fieldsInLastRow + "]");
                        return false;
                    }

                    fieldsInLastRow = fieldsInThisRow;
                }

                if (fieldsInLastRow > fieldsInFirstRow) {
                    terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because last row has more fields than first row: [" +
                        fieldsInFirstRow + "] and [" + fieldsInLastRow + "]");
                    return false;
                }
                if (fieldsInLastRow < fieldsInFirstRow) {
                    --numberOfRows;
                }
            } catch (SuperCsvException e) {
                // Tolerate an incomplete last row
                if (notUnexpectedEndOfFile(e)) {
                    terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because there was a parsing exception: [" +
                        e.getMessage() + "]");
                    return false;
                }
            }
            if (numberOfRows <= 1) {
                terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because fewer than 2 complete records in sample: [" +
                    numberOfRows + "]");
                return false;
            }
            terminal.println(Verbosity.VERBOSE, "Deciding sample is " + formatName);
            return true;

        } catch (IOException e) {
            terminal.println(Verbosity.VERBOSE, "Not " + formatName + " because there was a parsing exception: [" + e.getMessage() + "]");
            return false;
        }
    }

    private static boolean notUnexpectedEndOfFile(SuperCsvException e) {
        return e.getMessage().startsWith("unexpected end of file while reading quoted column") == false;
    }

    void createConfigs() {

        char delimiter = structure.getSeparator();
        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        if (structure.getTimestampField() != null) {
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getTimestampFormats(),
                structure.needClientTimezone(), false);
        }

        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE,
            makeFilebeatInputOptions(structure.getMultilineStartPattern(), structure.getExcludeLinesPattern()),
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        String logstashColumns = structure.getInputFields().stream()
            .map(column -> (column.indexOf('"') >= 0) ? ("'" + column + "'") : ("\"" + column + "\"")).collect(Collectors.joining(", "));
        String separatorIfRequired = (delimiter == ',') ? "" : String.format(Locale.ROOT, SEPARATOR_TEMPLATE, delimiter);
        String logstashColumnConversions = makeColumnConversions(structure.getMappings());
        String logstashStripFilter = Boolean.TRUE.equals(structure.getShouldTrimFields()) ?
            String.format(Locale.ROOT, LOGSTASH_STRIP_FILTER_TEMPLATE, logstashColumns) : "";
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, separatorIfRequired, logstashColumns,
            logstashColumnConversions, logstashStripFilter, logstashFromFilebeatDateFilter, elasticsearchHost);
        String skipHeaderIfRequired = structure.getHasHeaderRow() ? "    skip_header => true\n": "";
        logstashFromFileConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern()), separatorIfRequired, logstashColumns, skipHeaderIfRequired,
            logstashColumnConversions, logstashStripFilter, logstashFromFileDateFilter, elasticsearchHost, indexName);
    }

    String getFilebeatToLogstashConfig() {
        return filebeatToLogstashConfig;
    }

    String getLogstashFromFilebeatConfig() {
        return logstashFromFilebeatConfig;
    }

    String getLogstashFromFileConfig() {
        return logstashFromFileConfig;
    }

    @Override
    public synchronized void writeConfigs(Path directory) throws Exception {
        if (filebeatToLogstashConfig == null) {
            createConfigs();
            createPreambleComment();
        }

        writeMappingsConfigs(directory, structure.getMappings());

        writeConfigFile(directory, "filebeat-to-logstash.yml", filebeatToLogstashConfig);
        writeConfigFile(directory, "logstash-from-filebeat.conf", logstashFromFilebeatConfig);
        writeConfigFile(directory, "logstash-from-file.conf", logstashFromFileConfig);
    }

    static String makeColumnConversions(Map<String, Object> mappings) {
        StringBuilder builder = new StringBuilder();

        for (Map.Entry<String, Object> mapping : mappings.entrySet()) {

            if (mapping.getValue() instanceof Map == false) {
                continue;
            }

            String fieldName = mapping.getKey();
            @SuppressWarnings("unchecked")
            Map<String, String> settings = (Map<String, String>) mapping.getValue();
            if (settings.isEmpty()) {
                continue;
            }

            String convertTo = null;
            switch (settings.get(MAPPING_TYPE_SETTING)){

                case "boolean":
                    convertTo = "boolean";
                    break;
                case "byte":
                case "short":
                case "integer":
                case "long":
                    convertTo = "integer";
                    break;
                case "half_float":
                case "float":
                case "double":
                    convertTo = "float";
                    break;
            }

            if (convertTo != null) {
                String fieldQuote = bestLogstashQuoteFor(fieldName);
                builder.append("      ").append(fieldQuote).append(fieldName).append(fieldQuote)
                    .append(" => \"").append(convertTo).append("\"\n");
            }
        }

        return (builder.length() > 0) ? String.format(Locale.ROOT, LOGSTASH_CONVERSIONS_TEMPLATE, builder.toString()) : "";
    }
}
