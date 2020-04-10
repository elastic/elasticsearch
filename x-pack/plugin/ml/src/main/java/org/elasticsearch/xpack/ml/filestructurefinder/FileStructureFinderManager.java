/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Runs the high-level steps needed to create ingest configs for the specified file.  In order:
 * 1. Determine the most likely character set (UTF-8, UTF-16LE, ISO-8859-2, etc.)
 * 2. Load a sample of the file, consisting of the first 1000 lines of the file
 * 3. Determine the most likely file structure - one of NDJSON, XML, delimited or semi-structured text
 * 4. Create an appropriate structure object and delegate writing configs to it
 */
public final class FileStructureFinderManager {

    public static final int MIN_SAMPLE_LINE_COUNT = 2;
    public static final int DEFAULT_IDEAL_SAMPLE_LINE_COUNT = 1000;
    public static final int DEFAULT_LINE_MERGE_SIZE_LIMIT = 10000;

    static final Set<String> FILEBEAT_SUPPORTED_ENCODINGS = Set.of(
            "866",
            "ansi_x3.4-1968",
            "arabic",
            "ascii",
            "asmo-708",
            "big5",
            "big5-hkscs",
            "chinese",
            "cn-big5",
            "cp1250",
            "cp1251",
            "cp1252",
            "cp1253",
            "cp1254",
            "cp1255",
            "cp1256",
            "cp1257",
            "cp1258",
            "cp819",
            "cp866",
            "csbig5",
            "cseuckr",
            "cseucpkdfmtjapanese",
            "csgb2312",
            "csibm866",
            "csiso2022jp",
            "csiso2022kr",
            "csiso58gb231280",
            "csiso88596e",
            "csiso88596i",
            "csiso88598e",
            "csiso88598i",
            "csisolatin1",
            "csisolatin2",
            "csisolatin3",
            "csisolatin4",
            "csisolatin5",
            "csisolatin6",
            "csisolatin9",
            "csisolatinarabic",
            "csisolatincyrillic",
            "csisolatingreek",
            "csisolatinhebrew",
            "cskoi8r",
            "csksc56011987",
            "csmacintosh",
            "csshiftjis",
            "cyrillic",
            "dos-874",
            "ecma-114",
            "ecma-118",
            "elot_928",
            "euc-jp",
            "euc-kr",
            "gb18030",
            "gb2312",
            "gb_2312",
            "gb_2312-80",
            "gbk",
            "greek",
            "greek8",
            "hebrew",
            "hz-gb-2312",
            "ibm819",
            "ibm866",
            "iso-2022-cn",
            "iso-2022-cn-ext",
            "iso-2022-jp",
            "iso-2022-kr",
            "iso-8859-1",
            "iso-8859-10",
            "iso-8859-11",
            "iso-8859-13",
            "iso-8859-14",
            "iso-8859-15",
            "iso-8859-16",
            "iso-8859-2",
            "iso-8859-3",
            "iso-8859-4",
            "iso-8859-5",
            "iso-8859-6",
            "iso-8859-6-e",
            "iso-8859-6-i",
            "iso-8859-7",
            "iso-8859-8",
            "iso-8859-8-e",
            "iso-8859-8-i",
            "iso-8859-9",
            "iso-ir-100",
            "iso-ir-101",
            "iso-ir-109",
            "iso-ir-110",
            "iso-ir-126",
            "iso-ir-127",
            "iso-ir-138",
            "iso-ir-144",
            "iso-ir-148",
            "iso-ir-149",
            "iso-ir-157",
            "iso-ir-58",
            "iso8859-1",
            "iso8859-10",
            "iso8859-11",
            "iso8859-13",
            "iso8859-14",
            "iso8859-15",
            "iso8859-2",
            "iso8859-3",
            "iso8859-4",
            "iso8859-5",
            "iso8859-6",
            "iso8859-6e",
            "iso8859-6i",
            "iso8859-7",
            "iso8859-8",
            "iso8859-8e",
            "iso8859-8i",
            "iso8859-9",
            "iso88591",
            "iso885910",
            "iso885911",
            "iso885913",
            "iso885914",
            "iso885915",
            "iso88592",
            "iso88593",
            "iso88594",
            "iso88595",
            "iso88596",
            "iso88597",
            "iso88598",
            "iso88599",
            "iso_8859-1",
            "iso_8859-15",
            "iso_8859-1:1987",
            "iso_8859-2",
            "iso_8859-2:1987",
            "iso_8859-3",
            "iso_8859-3:1988",
            "iso_8859-4",
            "iso_8859-4:1988",
            "iso_8859-5",
            "iso_8859-5:1988",
            "iso_8859-6",
            "iso_8859-6:1987",
            "iso_8859-7",
            "iso_8859-7:1987",
            "iso_8859-8",
            "iso_8859-8:1988",
            "iso_8859-9",
            "iso_8859-9:1989",
            "koi",
            "koi8",
            "koi8-r",
            "koi8-ru",
            "koi8-u",
            "koi8_r",
            "korean",
            "ks_c_5601-1987",
            "ks_c_5601-1989",
            "ksc5601",
            "ksc_5601",
            "l1",
            "l2",
            "l3",
            "l4",
            "l5",
            "l6",
            "l9",
            "latin1",
            "latin2",
            "latin3",
            "latin4",
            "latin5",
            "latin6",
            "logical",
            "mac",
            "macintosh",
            "ms932",
            "ms_kanji",
            "shift-jis",
            "shift_jis",
            "sjis",
            "sun_eu_greek",
            "tis-620",
            "unicode-1-1-utf-8",
            "us-ascii",
            "utf-16",
            "utf-16-bom",
            "utf-16be",
            "utf-16be-bom",
            "utf-16le",
            "utf-16le-bom",
            "utf-8",
            "utf8",
            "visual",
            "windows-1250",
            "windows-1251",
            "windows-1252",
            "windows-1253",
            "windows-1254",
            "windows-1255",
            "windows-1256",
            "windows-1257",
            "windows-1258",
            "windows-31j",
            "windows-874",
            "windows-949",
            "x-cp1250",
            "x-cp1251",
            "x-cp1252",
            "x-cp1253",
            "x-cp1254",
            "x-cp1255",
            "x-cp1256",
            "x-cp1257",
            "x-cp1258",
            "x-euc-jp",
            "x-gbk",
            "x-mac-cyrillic",
            "x-mac-roman",
            "x-mac-ukrainian",
            "x-sjis",
            "x-x-big5");

    /**
     * These need to be ordered so that the more generic formats come after the more specific ones
     */
    private static final List<FileStructureFinderFactory> ORDERED_STRUCTURE_FACTORIES = List.of(
            // NDJSON will often also be valid (although utterly weird) CSV, so NDJSON must come before CSV
            new NdJsonFileStructureFinderFactory(),
            new XmlFileStructureFinderFactory(),
            new DelimitedFileStructureFinderFactory(',', '"', 2, false),
            new DelimitedFileStructureFinderFactory('\t', '"', 2, false),
            new DelimitedFileStructureFinderFactory(';', '"', 4, false),
            new DelimitedFileStructureFinderFactory('|', '"', 5, true),
            new TextLogFileStructureFinderFactory());

    private static final int BUFFER_SIZE = 8192;

    private final ScheduledExecutorService scheduler;

    /**
     * Create the file structure manager.
     * @param scheduler Used for checking timeouts.
     */
    public FileStructureFinderManager(ScheduledExecutorService scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler);
    }

    public FileStructureFinder findFileStructure(Integer idealSampleLineCount, Integer lineMergeSizeLimit,
                                                 InputStream fromFile) throws Exception {
        return findFileStructure(idealSampleLineCount, lineMergeSizeLimit, fromFile, FileStructureOverrides.EMPTY_OVERRIDES, null);
    }

    /**
     * Given a stream of data from some file, determine its structure.
     * @param idealSampleLineCount Ideally, how many lines from the stream will be read to determine the structure?
     *                             If the stream has fewer lines then an attempt will still be made, providing at
     *                             least {@link #MIN_SAMPLE_LINE_COUNT} lines can be read.  If <code>null</code>
     *                             the value of {@link #DEFAULT_IDEAL_SAMPLE_LINE_COUNT} will be used.
     * @param lineMergeSizeLimit Maximum number of characters permitted when lines are merged to create messages.
     *                           If <code>null</code> the value of {@link #DEFAULT_LINE_MERGE_SIZE_LIMIT} will be used.
     * @param fromFile A stream from which the sample will be read.
     * @param overrides Aspects of the file structure that are known in advance.  These take precedence over
     *                  values determined by structure analysis.  An exception will be thrown if the file structure
     *                  is incompatible with an overridden value.
     * @param timeout The maximum time the analysis is permitted to take.  If it takes longer than this an
     *                {@link ElasticsearchTimeoutException} may be thrown (although not necessarily immediately
     *                the timeout is exceeded).
     * @return A {@link FileStructureFinder} object from which the structure and messages can be queried.
     * @throws Exception A variety of problems could occur at various stages of the structure finding process.
     */
    public FileStructureFinder findFileStructure(Integer idealSampleLineCount, Integer lineMergeSizeLimit, InputStream fromFile,
                                                 FileStructureOverrides overrides, TimeValue timeout) throws Exception {
        return findFileStructure(new ArrayList<>(), (idealSampleLineCount == null) ? DEFAULT_IDEAL_SAMPLE_LINE_COUNT : idealSampleLineCount,
            (lineMergeSizeLimit == null) ? DEFAULT_LINE_MERGE_SIZE_LIMIT : lineMergeSizeLimit, fromFile, overrides, timeout);
    }

    public FileStructureFinder findFileStructure(List<String> explanation, int idealSampleLineCount, int lineMergeSizeLimit,
                                                 InputStream fromFile) throws Exception {
        return findFileStructure(explanation, idealSampleLineCount, lineMergeSizeLimit, fromFile, FileStructureOverrides.EMPTY_OVERRIDES,
            null);
    }

    public FileStructureFinder findFileStructure(List<String> explanation, int idealSampleLineCount, int lineMergeSizeLimit,
                                                 InputStream fromFile, FileStructureOverrides overrides,
                                                 TimeValue timeout) throws Exception {

        try (TimeoutChecker timeoutChecker = new TimeoutChecker("structure analysis", timeout, scheduler)) {

            String charsetName = overrides.getCharset();
            Reader sampleReader;
            if (charsetName != null) {
                // Creating the reader will throw if the specified character set does not exist
                sampleReader = new InputStreamReader(fromFile, charsetName);
                explanation.add("Using specified character encoding [" + charsetName + "]");
            } else {
                CharsetMatch charsetMatch = findCharset(explanation, fromFile, timeoutChecker);
                charsetName = charsetMatch.getName();
                sampleReader = charsetMatch.getReader();
            }

            Tuple<String, Boolean> sampleInfo = sampleFile(sampleReader, charsetName, MIN_SAMPLE_LINE_COUNT,
                Math.max(MIN_SAMPLE_LINE_COUNT, idealSampleLineCount), timeoutChecker);

            return makeBestStructureFinder(explanation, sampleInfo.v1(), charsetName, sampleInfo.v2(), lineMergeSizeLimit, overrides,
                timeoutChecker);
        } catch (Exception e) {
            // Add a dummy exception containing the explanation so far - this can be invaluable for troubleshooting as incorrect
            // decisions made early on in the structure analysis can result in seemingly crazy decisions or timeouts later on
            if (explanation.isEmpty() == false) {
                e.addSuppressed(
                    new ElasticsearchException(explanation.stream().collect(Collectors.joining("]\n[", "Explanation so far:\n[", "]\n"))));
            }
            throw e;
        }
    }

    CharsetMatch findCharset(List<String> explanation, InputStream inputStream, TimeoutChecker timeoutChecker) throws Exception {

        // We need an input stream that supports mark and reset, so wrap the argument
        // in a BufferedInputStream if it doesn't already support this feature
        if (inputStream.markSupported() == false) {
            inputStream = new BufferedInputStream(inputStream, BUFFER_SIZE);
        }

        // This is from ICU4J
        CharsetDetector charsetDetector = new CharsetDetector().setText(inputStream);
        CharsetMatch[] charsetMatches = charsetDetector.detectAll();
        timeoutChecker.check("character set detection");

        // Determine some extra characteristics of the input to compensate for some deficiencies of ICU4J
        boolean pureAscii = true;
        int evenPosZeroCount = 0;
        int oddPosZeroCount = 0;
        inputStream.mark(BUFFER_SIZE);
        byte[] workspace = new byte[BUFFER_SIZE];
        int remainingLength = BUFFER_SIZE;
        do {
            int bytesRead = inputStream.read(workspace, 0, remainingLength);
            if (bytesRead <= 0) {
                break;
            }
            for (int i = 0; i < bytesRead; ++i) {
                if (workspace[i] == 0) {
                    pureAscii = false;
                    if (i % 2 == 0) {
                        ++evenPosZeroCount;
                    } else {
                        ++oddPosZeroCount;
                    }
                } else {
                    pureAscii = pureAscii && workspace[i] > 0 && workspace[i] < 128;
                }
            }
            remainingLength -= bytesRead;
        } while (remainingLength > 0);
        inputStream.reset();
        boolean containsZeroBytes = evenPosZeroCount > 0 || oddPosZeroCount > 0;
        timeoutChecker.check("character set detection");

        if (pureAscii) {
            // If the input is pure ASCII then many single byte character sets will match.  We want to favour
            // UTF-8 in this case, as it avoids putting a bold declaration of a dubious character set choice
            // in the config files.
            Optional<CharsetMatch> utf8CharsetMatch = Arrays.stream(charsetMatches)
                .filter(charsetMatch -> StandardCharsets.UTF_8.name().equals(charsetMatch.getName())).findFirst();
            if (utf8CharsetMatch.isPresent()) {
                explanation.add("Using character encoding [" + StandardCharsets.UTF_8.name() +
                    "], which matched the input with [" + utf8CharsetMatch.get().getConfidence() + "%] confidence - first [" +
                    (BUFFER_SIZE / 1024) + "kB] of input was pure ASCII");
                return utf8CharsetMatch.get();
            }
        }

        // Input wasn't pure ASCII, so use the best matching character set that's supported by both Java and Go.
        // Additionally, if the input contains zero bytes then avoid single byte character sets, as ICU4J will
        // suggest these for binary files but then
        for (CharsetMatch charsetMatch : charsetMatches) {
            String name = charsetMatch.getName();
            if (Charset.isSupported(name) && FILEBEAT_SUPPORTED_ENCODINGS.contains(name.toLowerCase(Locale.ROOT))) {

                // This extra test is to avoid trying to read binary files as text.  Running the structure
                // finding algorithms on binary files is very slow as the binary files generally appear to
                // have very long lines.
                boolean spaceEncodingContainsZeroByte = false;
                Charset charset = Charset.forName(name);
                // Some character sets cannot be encoded.  These are extremely rare so it's likely that
                // they've been chosen based on incorrectly provided binary data.  Therefore, err on
                // the side of rejecting binary data.
                if (charset.canEncode()) {
                    byte[] spaceBytes = " ".getBytes(charset);
                    for (int i = 0; i < spaceBytes.length && spaceEncodingContainsZeroByte == false; ++i) {
                        spaceEncodingContainsZeroByte = (spaceBytes[i] == 0);
                    }
                }
                if (containsZeroBytes && spaceEncodingContainsZeroByte == false) {
                    explanation.add("Character encoding [" + name + "] matched the input with [" + charsetMatch.getConfidence() +
                        "%] confidence but was rejected as the input contains zero bytes and the [" + name + "] encoding does not");
                } else if (containsZeroBytes && 3 * oddPosZeroCount > 2 * evenPosZeroCount && 3 * evenPosZeroCount > 2 * oddPosZeroCount) {
                    explanation.add("Character encoding [" + name + "] matched the input with [" + charsetMatch.getConfidence() +
                        "%] confidence but was rejected as the distribution of zero bytes between odd and even positions in the " +
                        "file is very close - [" + evenPosZeroCount + "] and [" + oddPosZeroCount + "] in the first [" +
                        (BUFFER_SIZE / 1024) + "kB] of input");
                } else {
                    explanation.add("Using character encoding [" + name + "], which matched the input with [" +
                        charsetMatch.getConfidence() + "%] confidence");
                    return charsetMatch;
                }
            } else {
                explanation.add("Character encoding [" + name + "] matched the input with [" + charsetMatch.getConfidence() +
                    "%] confidence but was rejected as it is not supported by [" +
                    (Charset.isSupported(name) ? "Filebeat" : "the JVM") + "]");
            }
        }

        throw new IllegalArgumentException("Could not determine a usable character encoding for the input" +
            (containsZeroBytes ? " - could it be binary data?" : ""));
    }

    FileStructureFinder makeBestStructureFinder(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker,
                                                int lineMergeSizeLimit, FileStructureOverrides overrides,
                                                TimeoutChecker timeoutChecker) throws Exception {

        Character delimiter = overrides.getDelimiter();
        Character quote = overrides.getQuote();
        Boolean shouldTrimFields = overrides.getShouldTrimFields();
        List<FileStructureFinderFactory> factories;
        if (delimiter != null) {

            // If a precise delimiter is specified, we only need one structure finder
            // factory, and we'll tolerate as little as one column in the input
            factories = Collections.singletonList(new DelimitedFileStructureFinderFactory(delimiter, (quote == null) ? '"' : quote, 1,
                (shouldTrimFields == null) ? (delimiter == '|') : shouldTrimFields));

        } else if (quote != null || shouldTrimFields != null) {

            // The delimiter is not specified, but some other aspect of delimited files is,
            // so clone our default delimited factories altering the overridden values
            factories = ORDERED_STRUCTURE_FACTORIES.stream().filter(factory -> factory instanceof DelimitedFileStructureFinderFactory)
                .map(factory -> ((DelimitedFileStructureFinderFactory) factory).makeSimilar(quote, shouldTrimFields))
                .collect(Collectors.toList());

        } else {

            // We can use the default factories, but possibly filtered down to a specific format
            factories = ORDERED_STRUCTURE_FACTORIES.stream()
                .filter(factory -> factory.canFindFormat(overrides.getFormat())).collect(Collectors.toList());

        }

        for (FileStructureFinderFactory factory : factories) {
            timeoutChecker.check("high level format detection");
            if (factory.canCreateFromSample(explanation, sample)) {
                return factory.createFromSample(explanation, sample, charsetName, hasByteOrderMarker, lineMergeSizeLimit, overrides,
                    timeoutChecker);
            }
        }

        throw new IllegalArgumentException("Input did not match " +
            ((overrides.getFormat() == null) ? "any known formats" : "the specified format [" + overrides.getFormat() + "]"));
    }

    private Tuple<String, Boolean> sampleFile(Reader reader, String charsetName, int minLines, int maxLines, TimeoutChecker timeoutChecker)
        throws IOException {

        int lineCount = 0;
        BufferedReader bufferedReader = new BufferedReader(reader);
        StringBuilder sample = new StringBuilder();

        // Don't include any byte-order-marker in the sample.  (The logic to skip it works for both
        // UTF-8 and UTF-16 assuming the character set of the reader was correctly detected.)
        Boolean hasByteOrderMarker = null;
        if (charsetName.toUpperCase(Locale.ROOT).startsWith("UTF")) {
            int maybeByteOrderMarker = reader.read();
            hasByteOrderMarker = ((char) maybeByteOrderMarker == '\uFEFF');
            if (maybeByteOrderMarker >= 0 && hasByteOrderMarker == false && (char) maybeByteOrderMarker != '\r')
            {
                sample.appendCodePoint(maybeByteOrderMarker);
                if ((char) maybeByteOrderMarker == '\n') {
                    ++lineCount;
                }
            }
        }

        String line;
        while ((line = bufferedReader.readLine()) != null && ++lineCount <= maxLines) {
            sample.append(line).append('\n');
            timeoutChecker.check("sample line splitting");
        }

        if (lineCount < minLines) {
            throw new IllegalArgumentException("Input contained too few lines [" + lineCount + "] to obtain a meaningful sample");
        }

        return new Tuple<>(sample.toString(), hasByteOrderMarker);
    }
}
