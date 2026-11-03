/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Generates wide, flat, web-analytics style JSON documents, so string-parsing benchmarks exercise the same
 * code paths as a real bulk of such documents: many short fields, mostly small integers, and a few URL
 * strings full of {@code \/} escapes.
 *
 * <p>The shape is modelled on the first 100k documents of the ClickBench {@code hits} dataset, which was
 * written by PHP's {@code json_encode}, and measured as follows:
 * <ul>
 *   <li>105 fields per document: 38 string values (47% of them empty) and 67 integers.</li>
 *   <li>10.1% of string values contain a backslash escape, about 3.8 per document and 38% of all
 *       string-value bytes. 85% of the escapes are {@code \/} (the data was written by PHP's
 *       {@code json_encode}), 9% are unicode escapes (a backslash, {@code u} and four hex digits)
 *       and 6% are {@code \f}.</li>
 *   <li>{@code URL} is escaped in 99% of documents and {@code Referer} in 95%. Both begin with
 *       {@code http:\/\/} so the first escape sits about 5 bytes in, and they average roughly 4.5
 *       escapes each. {@code Title} is long (mean 226 bytes, raw UTF-8 Cyrillic) and escaped in 19%.</li>
 * </ul>
 * A document built by {@link #generate} reproduces those counts and the field order. It is not the
 * real data, so use {@link #load} with a real sample to validate any conclusion drawn from it.
 */
final class AnalyticsDocs {

    private AnalyticsDocs() {}

    /**
     * Field specs measured from the real data, in document order.
     * {@code Nz:d:n} is an integer that is zero {@code z}% of the time, otherwise has {@code d} digits and is
     * negative {@code n}% of the time. {@code Sp:l} is a string that is non-empty {@code p}% of the time
     * with mean length {@code l}. A few string fields are generated specially by name, see {@link #stringValue}.
     */
    private static final String[] FIELDS = {
        "WatchID:S100:19",
        "JavaEnable:N31:1:0",
        "Title:S99:226",
        "GoodEvent:N0:1:0",
        "EventTime:S100:19",
        "EventDate:S100:10",
        "CounterID:N0:2:0",
        "ClientIP:N0:10:32",
        "RegionID:N0:3:0",
        "UserID:S100:20",
        "CounterClass:N100:1:0",
        "OS:N1:2:0",
        "UserAgent:N1:1:0",
        "URL:S99:81",
        "Referer:S96:89",
        "IsRefresh:N51:1:0",
        "RefererCategoryID:N5:5:0",
        "RefererRegionID:N5:3:0",
        "URLCategoryID:N1:5:0",
        "URLRegionID:N1:3:0",
        "ResolutionWidth:N1:4:0",
        "ResolutionHeight:N1:3:0",
        "ResolutionDepth:N1:2:0",
        "FlashMajor:N11:2:0",
        "FlashMinor:N16:1:0",
        "FlashMinor2:S89:4",
        "NetMajor:N90:1:0",
        "NetMinor:N90:1:0",
        "UserAgentMajor:N1:2:0",
        "UserAgentMinor:S100:4",
        "CookieEnable:N2:1:0",
        "JavascriptEnable:N1:1:0",
        "IsMobile:N94:1:0",
        "MobilePhone:N95:1:0",
        "MobilePhoneModel:S3:4",
        "Params:S0:0",
        "IPNetworkID:N0:7:0",
        "TraficSourceID:N3:1:78",
        "SearchEngineID:N85:1:0",
        "SearchPhrase:S14:53",
        "AdvEngineID:N97:1:0",
        "IsArtifical:N51:1:0",
        "WindowClientWidth:N1:4:0",
        "WindowClientHeight:N1:3:0",
        "ClientTimeZone:N0:3:2",
        "ClientEventTime:S100:19",
        "SilverlightVersion1:N56:1:0",
        "SilverlightVersion2:N63:1:0",
        "SilverlightVersion3:N56:5:0",
        "SilverlightVersion4:N100:1:0",
        "PageCharset:S99:20",
        "CodeVersion:N1:4:0",
        "IsLink:N100:1:0",
        "IsDownload:N100:1:0",
        "IsNotBounce:N100:1:0",
        "FUniqID:S100:18",
        "OriginalURL:S5:128",
        "HID:N1:9:0",
        "IsOldCounter:N100:1:0",
        "IsEvent:N100:1:0",
        "IsParameter:N100:1:0",
        "DontCountHits:N100:1:0",
        "WithHash:N100:1:0",
        "HitColor:S100:1",
        "LocalEventTime:S100:19",
        "Age:N25:2:0",
        "Sex:N25:1:0",
        "Income:N28:1:0",
        "Interests:N34:2:0",
        "Robotness:N53:2:0",
        "RemoteIP:N0:10:32",
        "WindowName:N0:1:100",
        "OpenerName:N0:1:100",
        "HistoryLength:N0:1:100",
        "BrowserLanguage:S100:2",
        "BrowserCountry:S100:4",
        "SocialNetwork:S0:0",
        "SocialAction:S0:0",
        "HTTPError:N100:1:0",
        "SendTiming:N100:1:0",
        "DNSTiming:N97:1:0",
        "ConnectTiming:N93:2:0",
        "ResponseStartTiming:N70:3:0",
        "ResponseEndTiming:N71:2:0",
        "FetchTiming:N81:1:0",
        "SocialSourceNetworkID:N100:1:0",
        "SocialSourcePage:S0:7",
        "ParamPrice:S100:1",
        "ParamOrderID:S0:0",
        "ParamCurrency:S100:8",
        "ParamCurrencyID:N100:1:0",
        "OpenstatServiceName:S0:16",
        "OpenstatCampaignID:S0:7",
        "OpenstatAdID:S0:9",
        "OpenstatSourceID:S0:20",
        "UTMSource:S1:13",
        "UTMMedium:S1:4",
        "UTMCampaign:S1:24",
        "UTMContent:S1:6",
        "UTMTerm:S1:16",
        "FromTag:S0:0",
        "HasGCLID:N98:1:0",
        "RefererHash:S100:19",
        "URLHash:S100:19",
        "CLID:N100:1:0" };

    private static final String ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789";

    /** Generates one document in the shape described above. */
    static String generate(Random random) {
        StringBuilder sb = new StringBuilder(2800);
        sb.append('{');
        for (int i = 0; i < FIELDS.length; i++) {
            String spec = FIELDS[i];
            int colon = spec.indexOf(':');
            String name = spec.substring(0, colon);
            String[] p = spec.substring(colon + 2).split(":");
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(name).append("\":");
            if (spec.charAt(colon + 1) == 'N') {
                appendNumber(sb, random, Integer.parseInt(p[0]), Integer.parseInt(p[1]), Integer.parseInt(p[2]));
            } else {
                appendString(sb, random, name, Integer.parseInt(p[0]), Integer.parseInt(p[1]));
            }
        }
        sb.append('}');
        return sb.toString();
    }

    /** Generates {@code count} documents with a fixed seed. */
    static byte[][] generate(int count, long seed) {
        Random random = new Random(seed);
        byte[][] docs = new byte[count][];
        for (int i = 0; i < count; i++) {
            docs[i] = generate(random).getBytes(UTF_8);
        }
        return docs;
    }

    /**
     * Loads the first {@code count} lines of a newline-delimited JSON file, e.g. a real sample of the
     * data this class models. The bytes are used as they are: the sample is not always valid UTF-8.
     */
    static byte[][] load(Path file, int count) throws IOException {
        List<byte[]> docs = new ArrayList<>(count);
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
            int b;
            while (docs.size() < count && (b = in.read()) != -1) {
                if (b == '\n') {
                    addNonEmpty(docs, line);
                } else {
                    line.write(b);
                }
            }
        }
        addNonEmpty(docs, line);
        if (docs.size() < count) {
            throw new IllegalArgumentException("[" + file + "] has only " + docs.size() + " documents, need " + count);
        }
        return docs.toArray(new byte[0][]);
    }

    private static void addNonEmpty(List<byte[]> docs, ByteArrayOutputStream line) {
        if (line.size() > 0) {
            docs.add(line.toByteArray());
            line.reset();
        }
    }

    private static void appendNumber(StringBuilder sb, Random random, int zeroPercent, int digits, int negPercent) {
        if (random.nextInt(100) < zeroPercent) {
            sb.append('0');
            return;
        }
        if (random.nextInt(100) < negPercent) {
            sb.append('-');
        }
        sb.append((char) ('1' + random.nextInt(9)));
        for (int i = 1; i < digits; i++) {
            sb.append((char) ('0' + random.nextInt(10)));
        }
    }

    private static void appendString(StringBuilder sb, Random random, String name, int nonEmptyPercent, int meanLength) {
        sb.append('"');
        if (random.nextInt(100) < nonEmptyPercent) {
            stringValue(sb, random, name, meanLength);
        }
        sb.append('"');
    }

    private static void stringValue(StringBuilder sb, Random random, String name, int meanLength) {
        switch (name) {
            case "URL", "OriginalURL" -> url(sb, random, length(random, meanLength, 20));
            case "Referer" -> url(sb, random, length(random, meanLength, 20));
            case "Title" -> title(sb, random, length(random, meanLength, 20), random.nextInt(100) < 19);
            case "SearchPhrase" -> title(sb, random, length(random, meanLength, 10), false);
            case "BrowserCountry" -> {
                // A replacement character, followed by a \f escape in 62% of rows.
                sb.append('\uFFFD');
                if (random.nextInt(100) < 62) {
                    sb.append("\\f");
                }
            }
            case "ParamCurrency" -> sb.append("NH\\u001C");
            default -> {
                int len = Math.max(1, meanLength - meanLength / 4 + random.nextInt(Math.max(1, meanLength / 2)));
                for (int i = 0; i < len; i++) {
                    sb.append(ALNUM.charAt(random.nextInt(ALNUM.length())));
                }
            }
        }
    }

    /** Length with the given mean: {@code min} plus an exponentially distributed tail, capped. */
    private static int length(Random random, int mean, int min) {
        int tail = (int) (-Math.log(1.0 - random.nextDouble()) * Math.max(1, mean - min));
        return Math.min(min + tail, mean * 6);
    }

    /** {@code scheme:\/\/host(\/segment)+} with PHP-style escaped slashes, about 4.5 escapes per value. */
    private static void url(StringBuilder sb, Random random, int targetLength) {
        sb.append(random.nextInt(100) < 70 ? "http:" : "https:").append("\\/\\/");
        int hostLength = 6 + random.nextInt(14);
        for (int i = 0; i < hostLength; i++) {
            sb.append(i == hostLength - 4 ? '.' : ALNUM.charAt(random.nextInt(26)));
        }
        int segments = 1 + random.nextInt(4);
        int perSegment = Math.max(3, (targetLength - 12 - hostLength) / segments);
        for (int s = 0; s < segments; s++) {
            sb.append("\\/");
            int len = Math.max(1, perSegment - perSegment / 2 + random.nextInt(perSegment));
            for (int i = 0; i < len; i++) {
                sb.append(ALNUM.charAt(random.nextInt(ALNUM.length())));
            }
        }
    }

    /** Space-separated Cyrillic words (two bytes per letter in UTF-8), optionally with one {@code \/} escape. */
    private static void title(StringBuilder sb, Random random, int targetBytes, boolean withEscape) {
        int bytes = 0;
        boolean escaped = false;
        while (bytes < targetBytes) {
            int letters = 3 + random.nextInt(7);
            for (int i = 0; i < letters; i++) {
                sb.append((char) (0x0430 + random.nextInt(32)));
            }
            bytes += letters * 2;
            if (bytes < targetBytes) {
                if (withEscape && escaped == false && bytes > targetBytes / 2) {
                    sb.append("\\/");
                    escaped = true;
                    bytes += 2;
                } else {
                    sb.append(' ');
                    bytes++;
                }
            }
        }
    }
}
