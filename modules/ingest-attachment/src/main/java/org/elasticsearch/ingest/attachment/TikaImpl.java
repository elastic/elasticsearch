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
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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
        new org.apache.tika.parser.html.HtmlParser(),
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

    /**
     * parses with tika, throwing any exception hit while parsing the document
     */
    static String parse(final byte content[], final Metadata metadata, final int limit) throws TikaException, IOException {
        try {
            return TIKA_INSTANCE.parseToString(new ByteArrayInputStream(content), metadata, limit);
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
}
