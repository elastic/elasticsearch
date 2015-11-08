package org.elasticsearch.mapper.attachments;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.io.stream.StreamInput;

/** do NOT make public */
final class TikaImpl {
   
    /** subset of parsers for types we support */
    private static final Parser PARSERS[] = new Parser[] {
        // documents
        new org.apache.tika.parser.html.HtmlParser(),
        new org.apache.tika.parser.rtf.RTFParser(),
        new org.apache.tika.parser.pdf.PDFParser(),
        new org.apache.tika.parser.txt.TXTParser(),
        new org.apache.tika.parser.microsoft.OfficeParser(),
        new org.apache.tika.parser.microsoft.OldExcelParser(),
        new org.apache.tika.parser.microsoft.ooxml.OOXMLParser(),
        new org.apache.tika.parser.odf.OpenDocumentParser(),
        new org.apache.tika.parser.iwork.IWorkPackageParser(),
        new org.apache.tika.parser.xml.DcXMLParser(),
    };
    
    /** autodetector based on this subset */
    private static final AutoDetectParser PARSER_INSTANCE = new AutoDetectParser(PARSERS);
    
    /** singleton tika instance */
    private static final Tika TIKA_INSTANCE = new Tika(PARSER_INSTANCE.getDetector(), PARSER_INSTANCE);
    
    /**
     * parses with tika, throwing any exception hit while parsing the document
     */
    // only package private for testing!
    static String parse(final byte content[], final Metadata metadata, final int limit) throws TikaException, IOException {
        // check that its not unprivileged code like a script
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        try {
            return AccessController.doPrivileged(new PrivilegedExceptionAction<String>() {
                @Override
                public String run() throws TikaException, IOException {
                    return TIKA_INSTANCE.parseToString(StreamInput.wrap(content), metadata, limit);
                }
            });
        } catch (PrivilegedActionException e) {
            // checked exception from tika: unbox it
            Throwable cause = e.getCause();
            if (cause instanceof TikaException) {
                throw (TikaException) cause;
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new AssertionError(cause);
            }
        }
    }
}
