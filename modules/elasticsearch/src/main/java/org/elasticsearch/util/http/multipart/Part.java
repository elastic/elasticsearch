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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public abstract class Part {

    /**
     * Log object for this class.
     */
    private final static ESLogger LOG = Loggers.getLogger(Part.class);

    /**
     * The boundary
     */
    protected static final String BOUNDARY = "----------------314159265358979323846";

    /**
     * The boundary as a byte array.
     *
     * @deprecated
     */
    protected static final byte[] BOUNDARY_BYTES = MultipartEncodingUtil.getAsciiBytes(BOUNDARY);

    /**
     * The default boundary to be used if etBoundaryBytes(byte[]) has not
     * been called.
     */
    private static final byte[] DEFAULT_BOUNDARY_BYTES = BOUNDARY_BYTES;

    /**
     * Carriage return/linefeed
     */
    protected static final String CRLF = "\r\n";

    /**
     * Carriage return/linefeed as a byte array
     */
    protected static final byte[] CRLF_BYTES = MultipartEncodingUtil.getAsciiBytes(CRLF);

    /**
     * Content dispostion characters
     */
    protected static final String QUOTE = "\"";

    /**
     * Content dispostion as a byte array
     */
    protected static final byte[] QUOTE_BYTES =
            MultipartEncodingUtil.getAsciiBytes(QUOTE);

    /**
     * Extra characters
     */
    protected static final String EXTRA = "--";

    /**
     * Extra characters as a byte array
     */
    protected static final byte[] EXTRA_BYTES =
            MultipartEncodingUtil.getAsciiBytes(EXTRA);

    /**
     * Content dispostion characters
     */
    protected static final String CONTENT_DISPOSITION = "Content-Disposition: form-data; name=";

    /**
     * Content dispostion as a byte array
     */
    protected static final byte[] CONTENT_DISPOSITION_BYTES =
            MultipartEncodingUtil.getAsciiBytes(CONTENT_DISPOSITION);

    /**
     * Content type header
     */
    protected static final String CONTENT_TYPE = "Content-Type: ";

    /**
     * Content type header as a byte array
     */
    protected static final byte[] CONTENT_TYPE_BYTES =
            MultipartEncodingUtil.getAsciiBytes(CONTENT_TYPE);

    /**
     * Content charset
     */
    protected static final String CHARSET = "; charset=";

    /**
     * Content charset as a byte array
     */
    protected static final byte[] CHARSET_BYTES =
            MultipartEncodingUtil.getAsciiBytes(CHARSET);

    /**
     * Content type header
     */
    protected static final String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding: ";

    /**
     * Content type header as a byte array
     */
    protected static final byte[] CONTENT_TRANSFER_ENCODING_BYTES =
            MultipartEncodingUtil.getAsciiBytes(CONTENT_TRANSFER_ENCODING);

    /**
     * Return the boundary string.
     *
     * @return the boundary string
     * @deprecated uses a constant string. Rather use {@link #getPartBoundary}
     */
    public static String getBoundary() {
        return BOUNDARY;
    }

    /**
     * The ASCII bytes to use as the multipart boundary.
     */
    private byte[] boundaryBytes;

    /**
     * Return the name of this part.
     *
     * @return The name.
     */
    public abstract String getName();

    /**
     * Returns the content type of this part.
     *
     * @return the content type, or <code>null</code> to exclude the content type header
     */
    public abstract String getContentType();

    /**
     * Return the character encoding of this part.
     *
     * @return the character encoding, or <code>null</code> to exclude the character
     *         encoding header
     */
    public abstract String getCharSet();

    /**
     * Return the transfer encoding of this part.
     *
     * @return the transfer encoding, or <code>null</code> to exclude the transfer encoding header
     */
    public abstract String getTransferEncoding();

    /**
     * Gets the part boundary to be used.
     *
     * @return the part boundary as an array of bytes.
     * @since 3.0
     */
    protected byte[] getPartBoundary() {
        if (boundaryBytes == null) {
            // custom boundary bytes have not been set, use the default.
            return DEFAULT_BOUNDARY_BYTES;
        } else {
            return boundaryBytes;
        }
    }

    /**
     * Sets the part boundary.  Only meant to be used by
     * {@link Part#sendParts(java.io.OutputStream , Part[], byte[])}
     * and {@link Part#getLengthOfParts(Part[], byte[])}
     *
     * @param boundaryBytes An array of ASCII bytes.
     * @since 3.0
     */
    void setPartBoundary(byte[] boundaryBytes) {
        this.boundaryBytes = boundaryBytes;
    }

    /**
     * Tests if this part can be sent more than once.
     *
     * @return <code>true</code> if {@link #sendData(java.io.OutputStream)} can be successfully called
     *         more than once.
     * @since 3.0
     */
    public boolean isRepeatable() {
        return true;
    }

    /**
     * Write the start to the specified output stream
     *
     * @param out The output stream
     * @throws java.io.IOException If an IO problem occurs.
     */
    protected void sendStart(OutputStream out) throws IOException {
        LOG.trace("enter sendStart(OutputStream out)");
        out.write(EXTRA_BYTES);
        out.write(getPartBoundary());
        out.write(CRLF_BYTES);
    }

    /**
     * Write the content disposition header to the specified output stream
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected void sendDispositionHeader(OutputStream out) throws IOException {
        LOG.trace("enter sendDispositionHeader(OutputStream out)");
        out.write(CONTENT_DISPOSITION_BYTES);
        out.write(QUOTE_BYTES);
        out.write(MultipartEncodingUtil.getAsciiBytes(getName()));
        out.write(QUOTE_BYTES);
    }

    /**
     * Write the content type header to the specified output stream
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected void sendContentTypeHeader(OutputStream out) throws IOException {
        LOG.trace("enter sendContentTypeHeader(OutputStream out)");
        String contentType = getContentType();
        if (contentType != null) {
            out.write(CRLF_BYTES);
            out.write(CONTENT_TYPE_BYTES);
            out.write(MultipartEncodingUtil.getAsciiBytes(contentType));
            String charSet = getCharSet();
            if (charSet != null) {
                out.write(CHARSET_BYTES);
                out.write(MultipartEncodingUtil.getAsciiBytes(charSet));
            }
        }
    }

    /**
     * Write the content transfer encoding header to the specified
     * output stream
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected void sendTransferEncodingHeader(OutputStream out) throws IOException {
        LOG.trace("enter sendTransferEncodingHeader(OutputStream out)");
        String transferEncoding = getTransferEncoding();
        if (transferEncoding != null) {
            out.write(CRLF_BYTES);
            out.write(CONTENT_TRANSFER_ENCODING_BYTES);
            out.write(MultipartEncodingUtil.getAsciiBytes(transferEncoding));
        }
    }

    /**
     * Write the end of the header to the output stream
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected void sendEndOfHeader(OutputStream out) throws IOException {
        LOG.trace("enter sendEndOfHeader(OutputStream out)");
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    /**
     * Write the data to the specified output stream
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected abstract void sendData(OutputStream out) throws IOException;

    /**
     * Return the length of the main content
     *
     * @return long The length.
     * @throws IOException If an IO problem occurs
     */
    protected abstract long lengthOfData() throws IOException;

    /**
     * Write the end data to the output stream.
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    protected void sendEnd(OutputStream out) throws IOException {
        LOG.trace("enter sendEnd(OutputStream out)");
        out.write(CRLF_BYTES);
    }

    /**
     * Write all the data to the output stream.
     * If you override this method make sure to override
     * #length() as well
     *
     * @param out The output stream
     * @throws IOException If an IO problem occurs.
     */
    public void send(OutputStream out) throws IOException {
        LOG.trace("enter send(OutputStream out)");
        sendStart(out);
        sendDispositionHeader(out);
        sendContentTypeHeader(out);
        sendTransferEncodingHeader(out);
        sendEndOfHeader(out);
        sendData(out);
        sendEnd(out);
    }


    /**
     * Return the full length of all the data.
     * If you override this method make sure to override
     * #send(OutputStream) as well
     *
     * @return long The length.
     * @throws IOException If an IO problem occurs
     */
    public long length() throws IOException {
        LOG.trace("enter length()");
        if (lengthOfData() < 0) {
            return -1;
        }
        ByteArrayOutputStream overhead = new ByteArrayOutputStream();
        sendStart(overhead);
        sendDispositionHeader(overhead);
        sendContentTypeHeader(overhead);
        sendTransferEncodingHeader(overhead);
        sendEndOfHeader(overhead);
        sendEnd(overhead);
        return overhead.size() + lengthOfData();
    }

    /**
     * Return a string representation of this object.
     *
     * @return A string representation of this object.
     * @see java.lang.Object#toString()
     */
    public String toString() {
        return this.getName();
    }

    /**
     * Write all parts and the last boundary to the specified output stream.
     *
     * @param out   The stream to write to.
     * @param parts The parts to write.
     * @throws IOException If an I/O error occurs while writing the parts.
     */
    public static void sendParts(OutputStream out, final Part[] parts)
            throws IOException {
        sendParts(out, parts, DEFAULT_BOUNDARY_BYTES);
    }

    /**
     * Write all parts and the last boundary to the specified output stream.
     *
     * @param out          The stream to write to.
     * @param parts        The parts to write.
     * @param partBoundary The ASCII bytes to use as the part boundary.
     * @throws IOException If an I/O error occurs while writing the parts.
     * @since 3.0
     */
    public static void sendParts(OutputStream out, Part[] parts, byte[] partBoundary)
            throws IOException {

        if (parts == null) {
            throw new IllegalArgumentException("Parts may not be null");
        }
        if (partBoundary == null || partBoundary.length == 0) {
            throw new IllegalArgumentException("partBoundary may not be empty");
        }
        for (int i = 0; i < parts.length; i++) {
            // set the part boundary before the part is sent
            parts[i].setPartBoundary(partBoundary);
            parts[i].send(out);
        }
        out.write(EXTRA_BYTES);
        out.write(partBoundary);
        out.write(EXTRA_BYTES);
        out.write(CRLF_BYTES);
    }

    /**
     * Return the total sum of all parts and that of the last boundary
     *
     * @param parts The parts.
     * @return The total length
     * @throws IOException If an I/O error occurs while writing the parts.
     */
    public static long getLengthOfParts(Part[] parts)
            throws IOException {
        return getLengthOfParts(parts, DEFAULT_BOUNDARY_BYTES);
    }

    /**
     * Gets the length of the multipart message including the given parts.
     *
     * @param parts        The parts.
     * @param partBoundary The ASCII bytes to use as the part boundary.
     * @return The total length
     * @throws IOException If an I/O error occurs while writing the parts.
     * @since 3.0
     */
    public static long getLengthOfParts(Part[] parts, byte[] partBoundary) throws IOException {
        LOG.trace("getLengthOfParts(Parts[])");
        if (parts == null) {
            throw new IllegalArgumentException("Parts may not be null");
        }
        long total = 0;
        for (int i = 0; i < parts.length; i++) {
            // set the part boundary before we calculate the part's length
            parts[i].setPartBoundary(partBoundary);
            long l = parts[i].length();
            if (l < 0) {
                return -1;
            }
            total += l;
        }
        total += EXTRA_BYTES.length;
        total += partBoundary.length;
        total += EXTRA_BYTES.length;
        total += CRLF_BYTES.length;
        return total;
    }
}
