/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.filter.DateNormalizingMetadataFilter;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.parser.html.JSoupParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs tika with limited parsers and limited permissions.
 * <p>
 * Do NOT make public
 */
final class TikaImpl {

    /** Exclude some formats */
    private static final Set<MediaType> EXCLUDES = new HashSet<>(
        Arrays.asList(
            MediaType.application("vnd.ms-visio.drawing"),
            MediaType.application("vnd.ms-visio.drawing.macroenabled.12"),
            MediaType.application("vnd.ms-visio.stencil"),
            MediaType.application("vnd.ms-visio.stencil.macroenabled.12"),
            MediaType.application("vnd.ms-visio.template"),
            MediaType.application("vnd.ms-visio.template.macroenabled.12"),
            MediaType.application("vnd.ms-visio.drawing")
        )
    );

    /** subset of parsers for types we support */
    private static final Parser PARSERS[] = new Parser[] {
        // documents
        new JSoupParser(),
        new org.apache.tika.parser.microsoft.rtf.RTFParser(),
        new org.apache.tika.parser.pdf.PDFParser(),
        new org.apache.tika.parser.txt.TXTParser(),
        new org.apache.tika.parser.microsoft.OfficeParser(),
        new org.apache.tika.parser.microsoft.OldExcelParser(),
        ParserDecorator.withoutTypes(new org.apache.tika.parser.microsoft.ooxml.OOXMLParser(), EXCLUDES),
        new org.apache.tika.parser.odf.OpenDocumentParser(),
        new org.apache.tika.parser.iwork.IWorkPackageParser(),
        new org.apache.tika.parser.xml.DcXMLParser(),
        new org.apache.tika.parser.epub.EpubParser(), };

    /** autodetector based on this subset */
    private static final AutoDetectParser PARSER_INSTANCE = new AutoDetectParser(PARSERS);

    /** singleton tika instance */
    private static final Tika TIKA_INSTANCE = new Tika(PARSER_INSTANCE.getDetector(), PARSER_INSTANCE);

    /** tika decodes under vendor supersets; these two are JVM-internal names with no IANA registration */
    private static final Map<String, String> IANA_CHARSET_NAMES = Map.of("x-eucJP-Open", "EUC-JP", "x-windows-949", "EUC-KR");

    /** rewrites timezone-less dates as UTC so they always index as dates */
    private static final DateNormalizingMetadataFilter DATE_FILTER = new DateNormalizingMetadataFilter();

    /** date-times carrying an explicit offset, with or without a colon; DATE_FILTER would ignore the offset */
    private static final DateTimeFormatter OFFSET_DATE_TIME = new DateTimeFormatterBuilder().append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        .optionalStart()
        .appendOffset("+HH:MM", "Z")
        .optionalEnd()
        .optionalStart()
        .appendOffset("+HHMM", "Z")
        .optionalEnd()
        .toFormatter(Locale.ROOT);

    /**
     * parses with tika, throwing any exception hit while parsing the document
     */
    static String parse(final byte content[], final Metadata metadata, final int limit) throws TikaException, IOException {
        try {
            String text = TIKA_INSTANCE.parseToString(new ByteArrayInputStream(content), metadata, limit);
            normalizeDates(metadata);
            normalizeCharsetName(metadata);
            return text;
        } catch (LinkageError e) {
            if (e.getMessage().contains("bouncycastle")) {
                /*
                 * Elasticsearch does not ship with bouncycastle. It is only used for public-key-encrypted PDFs, which this module does
                 * not support anyway.
                 */
                throw new RuntimeException("document is encrypted", e);
            }
            throw new RuntimeException(e);
        }
    }

    static void normalizeCharsetName(Metadata metadata) {
        String contentType = metadata.get(HttpHeaders.CONTENT_TYPE);
        MediaType mediaType = contentType == null ? null : MediaType.parse(contentType);
        if (mediaType == null) {
            return;
        }
        String charset = mediaType.getParameters().get("charset");
        String iana = charset == null ? null : IANA_CHARSET_NAMES.get(charset);
        if (iana != null) {
            Map<String, String> parameters = new HashMap<>(mediaType.getParameters());
            parameters.put("charset", iana);
            metadata.set(HttpHeaders.CONTENT_TYPE, new MediaType(mediaType.getBaseType(), parameters).toString());
        }
    }

    static void normalizeDates(Metadata metadata) throws TikaException {
        for (String name : metadata.names()) {
            Property property = Property.get(name);
            if (property == null || property.getValueType() != Property.ValueType.DATE) {
                continue;
            }
            String value = metadata.get(name);
            if (value == null) {
                continue;
            }
            // tika 4.0.x and 4.1.x render a malformed year such as "0-01-01" as a negative year
            if (value.startsWith("-")) {
                metadata.remove(name);
                continue;
            }
            if (value.endsWith("Z") == false) {
                try {
                    OffsetDateTime parsed = OffsetDateTime.parse(value, OFFSET_DATE_TIME);
                    metadata.set(property, parsed.toInstant().truncatedTo(ChronoUnit.SECONDS).toString());
                } catch (DateTimeParseException e) {
                    // no offset (or not a date-time at all): leave it to DATE_FILTER
                }
            }
        }
        DATE_FILTER.filter(List.of(metadata));
    }
}
