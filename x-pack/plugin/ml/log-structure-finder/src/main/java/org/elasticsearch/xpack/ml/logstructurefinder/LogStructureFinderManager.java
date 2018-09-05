/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import com.ibm.icu.text.CharsetDetector;
import com.ibm.icu.text.CharsetMatch;
import org.elasticsearch.common.collect.Tuple;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Runs the high-level steps needed to create ingest configs for the specified log file.  In order:
 * 1. Determine the most likely character set (UTF-8, UTF-16LE, ISO-8859-2, etc.)
 * 2. Load a sample of the file, consisting of the first 1000 lines of the file
 * 3. Determine the most likely file structure - one of ND-JSON, XML, CSV, TSV or semi-structured text
 * 4. Create an appropriate structure object and delegate writing configs to it
 */
public final class LogStructureFinderManager {

    public static final int MIN_SAMPLE_LINE_COUNT = 2;

    static final Set<String> FILEBEAT_SUPPORTED_ENCODINGS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "866", "ansi_x3.4-1968", "arabic", "ascii", "asmo-708", "big5", "big5-hkscs", "chinese", "cn-big5", "cp1250", "cp1251", "cp1252",
        "cp1253", "cp1254", "cp1255", "cp1256", "cp1257", "cp1258", "cp819", "cp866", "csbig5", "cseuckr", "cseucpkdfmtjapanese",
        "csgb2312", "csibm866", "csiso2022jp", "csiso2022kr", "csiso58gb231280", "csiso88596e", "csiso88596i", "csiso88598e", "csiso88598i",
        "csisolatin1", "csisolatin2", "csisolatin3", "csisolatin4", "csisolatin5", "csisolatin6", "csisolatin9", "csisolatinarabic",
        "csisolatincyrillic", "csisolatingreek", "csisolatinhebrew", "cskoi8r", "csksc56011987", "csmacintosh", "csshiftjis", "cyrillic",
        "dos-874", "ecma-114", "ecma-118", "elot_928", "euc-jp", "euc-kr", "gb18030", "gb2312", "gb_2312", "gb_2312-80", "gbk", "greek",
        "greek8", "hebrew", "hz-gb-2312", "ibm819", "ibm866", "iso-2022-cn", "iso-2022-cn-ext", "iso-2022-jp", "iso-2022-kr", "iso-8859-1",
        "iso-8859-10", "iso-8859-11", "iso-8859-13", "iso-8859-14", "iso-8859-15", "iso-8859-16", "iso-8859-2", "iso-8859-3", "iso-8859-4",
        "iso-8859-5", "iso-8859-6", "iso-8859-6-e", "iso-8859-6-i", "iso-8859-7", "iso-8859-8", "iso-8859-8-e", "iso-8859-8-i",
        "iso-8859-9", "iso-ir-100", "iso-ir-101", "iso-ir-109", "iso-ir-110", "iso-ir-126", "iso-ir-127", "iso-ir-138", "iso-ir-144",
        "iso-ir-148", "iso-ir-149", "iso-ir-157", "iso-ir-58", "iso8859-1", "iso8859-10", "iso8859-11", "iso8859-13", "iso8859-14",
        "iso8859-15", "iso8859-2", "iso8859-3", "iso8859-4", "iso8859-5", "iso8859-6", "iso8859-6e", "iso8859-6i", "iso8859-7", "iso8859-8",
        "iso8859-8e", "iso8859-8i", "iso8859-9", "iso88591", "iso885910", "iso885911", "iso885913", "iso885914", "iso885915", "iso88592",
        "iso88593", "iso88594", "iso88595", "iso88596", "iso88597", "iso88598", "iso88599", "iso_8859-1", "iso_8859-15", "iso_8859-1:1987",
        "iso_8859-2", "iso_8859-2:1987", "iso_8859-3", "iso_8859-3:1988", "iso_8859-4", "iso_8859-4:1988", "iso_8859-5", "iso_8859-5:1988",
        "iso_8859-6", "iso_8859-6:1987", "iso_8859-7", "iso_8859-7:1987", "iso_8859-8", "iso_8859-8:1988", "iso_8859-9", "iso_8859-9:1989",
        "koi", "koi8", "koi8-r", "koi8-ru", "koi8-u", "koi8_r", "korean", "ks_c_5601-1987", "ks_c_5601-1989", "ksc5601", "ksc_5601", "l1",
        "l2", "l3", "l4", "l5", "l6", "l9", "latin1", "latin2", "latin3", "latin4", "latin5", "latin6", "logical", "mac", "macintosh",
        "ms932", "ms_kanji", "shift-jis", "shift_jis", "sjis", "sun_eu_greek", "tis-620", "unicode-1-1-utf-8", "us-ascii", "utf-16",
        "utf-16-bom", "utf-16be", "utf-16be-bom", "utf-16le", "utf-16le-bom", "utf-8", "utf8", "visual", "windows-1250", "windows-1251",
        "windows-1252", "windows-1253", "windows-1254", "windows-1255", "windows-1256", "windows-1257", "windows-1258", "windows-31j",
        "windows-874", "windows-949", "x-cp1250", "x-cp1251", "x-cp1252", "x-cp1253", "x-cp1254", "x-cp1255", "x-cp1256", "x-cp1257",
        "x-cp1258", "x-euc-jp", "x-gbk", "x-mac-cyrillic", "x-mac-roman", "x-mac-ukrainian", "x-sjis", "x-x-big5"
    )));

    /**
     * These need to be ordered so that the more generic formats come after the more specific ones
     */
    private static final List<LogStructureFinderFactory> ORDERED_STRUCTURE_FACTORIES = Collections.unmodifiableList(Arrays.asList(
        new JsonLogStructureFinderFactory(),
        new XmlLogStructureFinderFactory(),
        // ND-JSON will often also be valid (although utterly weird) CSV, so JSON must come before CSV
        new DelimitedLogStructureFinderFactory(',', 2, false),
        new DelimitedLogStructureFinderFactory('\t', 2, false),
        new DelimitedLogStructureFinderFactory(';', 4, false),
        new DelimitedLogStructureFinderFactory('|', 5, true),
        new TextLogStructureFinderFactory()
    ));

    private static final int BUFFER_SIZE = 8192;

    /**
     * Given a stream of data from some log file, determine its structure.
     * @param idealSampleLineCount Ideally, how many lines from the stream will be read to determine the structure?
     *                             If the stream has fewer lines then an attempt will still be made, providing at
     *                             least {@link #MIN_SAMPLE_LINE_COUNT} lines can be read.
     * @param fromFile A stream from which the sample will be read.
     * @return A {@link LogStructureFinder} object from which the structure and messages can be queried.
     * @throws Exception A variety of problems could occur at various stages of the structure finding process.
     */
    public LogStructureFinder findLogStructure(int idealSampleLineCount, InputStream fromFile) throws Exception {
        return findLogStructure(new ArrayList<>(), idealSampleLineCount, fromFile);
    }

    public LogStructureFinder findLogStructure(List<String> explanation, int idealSampleLineCount, InputStream fromFile)
        throws Exception {

        CharsetMatch charsetMatch = findCharset(explanation, fromFile);
        String charsetName = charsetMatch.getName();

        Tuple<String, Boolean> sampleInfo = sampleFile(charsetMatch.getReader(), charsetName, MIN_SAMPLE_LINE_COUNT,
            Math.max(MIN_SAMPLE_LINE_COUNT, idealSampleLineCount));

        return makeBestStructureFinder(explanation, sampleInfo.v1(), charsetName, sampleInfo.v2());
    }

    CharsetMatch findCharset(List<String> explanation, InputStream inputStream) throws Exception {

        // We need an input stream that supports mark and reset, so wrap the argument
        // in a BufferedInputStream if it doesn't already support this feature
        if (inputStream.markSupported() == false) {
            inputStream = new BufferedInputStream(inputStream, BUFFER_SIZE);
        }

        // This is from ICU4J
        CharsetDetector charsetDetector = new CharsetDetector().setText(inputStream);
        CharsetMatch[] charsetMatches = charsetDetector.detectAll();

        // Determine some extra characteristics of the input to compensate for some deficiencies of ICU4J
        boolean pureAscii = true;
        boolean containsZeroBytes = false;
        inputStream.mark(BUFFER_SIZE);
        byte[] workspace = new byte[BUFFER_SIZE];
        int remainingLength = BUFFER_SIZE;
        do {
            int bytesRead = inputStream.read(workspace, 0, remainingLength);
            if (bytesRead <= 0) {
                break;
            }
            for (int i = 0; i < bytesRead && containsZeroBytes == false; ++i) {
                if (workspace[i] == 0) {
                    containsZeroBytes = true;
                    pureAscii = false;
                } else {
                    pureAscii = pureAscii && workspace[i] > 0 && workspace[i] < 128;
                }
            }
            remainingLength -= bytesRead;
        } while (containsZeroBytes == false && remainingLength > 0);
        inputStream.reset();

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

                // This extra test is to avoid trying to read binary files as text.  Running the log config
                // deduction algorithms on binary files is very slow as the binary files generally appear to
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

    LogStructureFinder makeBestStructureFinder(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker)
        throws Exception {

        for (LogStructureFinderFactory factory : ORDERED_STRUCTURE_FACTORIES) {
            if (factory.canCreateFromSample(explanation, sample)) {
                return factory.createFromSample(explanation, sample, charsetName, hasByteOrderMarker);
            }
        }
        throw new IllegalArgumentException("Input did not match any known formats");
    }

    private Tuple<String, Boolean> sampleFile(Reader reader, String charsetName, int minLines, int maxLines) throws IOException {

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
        }

        if (lineCount < minLines) {
            throw new IllegalArgumentException("Input contained too few lines to sample");
        }

        return new Tuple<>(sample.toString(), hasByteOrderMarker);
    }
}
