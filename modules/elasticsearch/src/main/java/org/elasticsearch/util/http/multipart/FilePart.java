/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.http.multipart;

import org.elasticsearch.util.logging.ESLogger;
import org.elasticsearch.util.logging.Loggers;

import java.io.*;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class FilePart extends PartBase {

    /**
     * Default content encoding of file attachments.
     */
    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    /**
     * Default charset of file attachments.
     */
    public static final String DEFAULT_CHARSET = "ISO-8859-1";

    /**
     * Default transfer encoding of file attachments.
     */
    public static final String DEFAULT_TRANSFER_ENCODING = "binary";

    /**
     * Log object for this class.
     */
    private final static ESLogger LOG = Loggers.getLogger(FilePart.class);

    /**
     * Attachment's file name
     */
    protected static final String FILE_NAME = "; filename=";

    /**
     * Attachment's file name as a byte array
     */
    private static final byte[] FILE_NAME_BYTES =
            MultipartEncodingUtil.getAsciiBytes(FILE_NAME);

    /**
     * Source of the file part.
     */
    private PartSource source;

    /**
     * FilePart Constructor.
     *
     * @param name        the name for this part
     * @param partSource  the source for this part
     * @param contentType the content type for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CONTENT_TYPE default} is used
     * @param charset     the charset encoding for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CHARSET default} is used
     */
    public FilePart(String name, PartSource partSource, String contentType, String charset) {

        super(
                name,
                contentType == null ? DEFAULT_CONTENT_TYPE : contentType,
                charset == null ? "ISO-8859-1" : charset,
                DEFAULT_TRANSFER_ENCODING
        );

        if (partSource == null) {
            throw new IllegalArgumentException("Source may not be null");
        }
        this.source = partSource;
    }

    /**
     * FilePart Constructor.
     *
     * @param name       the name for this part
     * @param partSource the source for this part
     */
    public FilePart(String name, PartSource partSource) {
        this(name, partSource, null, null);
    }

    /**
     * FilePart Constructor.
     *
     * @param name the name of the file part
     * @param file the file to post
     * @throws java.io.FileNotFoundException if the <i>file</i> is not a normal
     *                                       file or if it is not readable.
     */
    public FilePart(String name, File file)
            throws FileNotFoundException {
        this(name, new FilePartSource(file), null, null);
    }

    /**
     * FilePart Constructor.
     *
     * @param name        the name of the file part
     * @param file        the file to post
     * @param contentType the content type for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CONTENT_TYPE default} is used
     * @param charset     the charset encoding for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CHARSET default} is used
     * @throws FileNotFoundException if the <i>file</i> is not a normal
     *                               file or if it is not readable.
     */
    public FilePart(String name, File file, String contentType, String charset)
            throws FileNotFoundException {
        this(name, new FilePartSource(file), contentType, charset);
    }

    /**
     * FilePart Constructor.
     *
     * @param name     the name of the file part
     * @param fileName the file name
     * @param file     the file to post
     * @throws FileNotFoundException if the <i>file</i> is not a normal
     *                               file or if it is not readable.
     */
    public FilePart(String name, String fileName, File file)
            throws FileNotFoundException {
        this(name, new FilePartSource(fileName, file), null, null);
    }

    /**
     * FilePart Constructor.
     *
     * @param name        the name of the file part
     * @param fileName    the file name
     * @param file        the file to post
     * @param contentType the content type for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CONTENT_TYPE default} is used
     * @param charset     the charset encoding for this part, if <code>null</code> the
     *                    {@link #DEFAULT_CHARSET default} is used
     * @throws FileNotFoundException if the <i>file</i> is not a normal
     *                               file or if it is not readable.
     */
    public FilePart(String name, String fileName, File file, String contentType, String charset)
            throws FileNotFoundException {
        this(name, new FilePartSource(fileName, file), contentType, charset);
    }

    /**
     * Write the disposition header to the output stream
     *
     * @param out The output stream
     * @throws java.io.IOException If an IO problem occurs
     */
    protected void sendDispositionHeader(OutputStream out)
            throws IOException {
        LOG.trace("enter sendDispositionHeader(OutputStream out)");
        super.sendDispositionHeader(out);
        String filename = this.source.getFileName();
        if (filename != null) {
            out.write(FILE_NAME_BYTES);
            out.write(QUOTE_BYTES);
            out.write(MultipartEncodingUtil.getAsciiBytes(filename));
            out.write(QUOTE_BYTES);
        }
    }

    /**
     * Write the data in "source" to the specified stream.
     *
     * @param out The output stream.
     * @throws IOException if an IO problem occurs.
     */
    protected void sendData(OutputStream out) throws IOException {
        LOG.trace("enter sendData(OutputStream out)");
        if (lengthOfData() == 0) {

            // this file contains no data, so there is nothing to send.
            // we don't want to create a zero length buffer as this will
            // cause an infinite loop when reading.
            LOG.debug("No data to send.");
            return;
        }

        byte[] tmp = new byte[4096];
        InputStream instream = source.createInputStream();
        try {
            int len;
            while ((len = instream.read(tmp)) >= 0) {
                out.write(tmp, 0, len);
            }
        } finally {
            // we're done with the stream, close it
            instream.close();
        }
    }

    /**
     * Returns the source of the file part.
     *
     * @return The source.
     */
    protected PartSource getSource() {
        LOG.trace("enter getSource()");
        return this.source;
    }

    /**
     * Return the length of the data.
     *
     * @return The length.
     * @throws IOException if an IO problem occurs
     */
    protected long lengthOfData() throws IOException {
        LOG.trace("enter lengthOfData()");
        return source.getLength();
    }

}
