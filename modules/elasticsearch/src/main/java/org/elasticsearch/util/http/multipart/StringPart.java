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

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class StringPart extends PartBase {

    /**
     * Log object for this class.
     */
    private final static ESLogger LOG = Loggers.getLogger(StringPart.class);

    /**
     * Default content encoding of string parameters.
     */
    public static final String DEFAULT_CONTENT_TYPE = "text/plain";

    /**
     * Default charset of string parameters
     */
    public static final String DEFAULT_CHARSET = "US-ASCII";

    /**
     * Default transfer encoding of string parameters
     */
    public static final String DEFAULT_TRANSFER_ENCODING = "8bit";

    /**
     * Contents of this StringPart.
     */
    private byte[] content;

    /**
     * The String value of this part.
     */
    private String value;

    /**
     * Constructor.
     *
     * @param name    The name of the part
     * @param value   the string to post
     * @param charset the charset to be used to encode the string, if <code>null</code>
     *                the {@link #DEFAULT_CHARSET default} is used
     */
    public StringPart(String name, String value, String charset) {

        super(
                name,
                DEFAULT_CONTENT_TYPE,
                charset == null ? DEFAULT_CHARSET : charset,
                DEFAULT_TRANSFER_ENCODING
        );
        if (value == null) {
            throw new IllegalArgumentException("Value may not be null");
        }
        if (value.indexOf(0) != -1) {
            // See RFC 2048, 2.8. "8bit Data"
            throw new IllegalArgumentException("NULs may not be present in string parts");
        }
        this.value = value;
    }

    /**
     * Constructor.
     *
     * @param name  The name of the part
     * @param value the string to post
     */
    public StringPart(String name, String value) {
        this(name, value, null);
    }

    /**
     * Gets the content in bytes.  Bytes are lazily created to allow the charset to be changed
     * after the part is created.
     *
     * @return the content in bytes
     */
    private byte[] getContent() {
        if (content == null) {
            content = MultipartEncodingUtil.getBytes(value, getCharSet());
        }
        return content;
    }

    /**
     * Writes the data to the given OutputStream.
     *
     * @param out the OutputStream to write to
     * @throws java.io.IOException if there is a write error
     */
    protected void sendData(OutputStream out) throws IOException {
        LOG.trace("enter sendData(OutputStream)");
        out.write(getContent());
    }

    /**
     * Return the length of the data.
     *
     * @return The length of the data.
     * @throws IOException If an IO problem occurs
     */
    protected long lengthOfData() throws IOException {
        LOG.trace("enter lengthOfData()");
        return getContent().length;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.methods.multipart.BasePart#setCharSet(java.lang.String)
     */

    public void setCharSet(String charSet) {
        super.setCharSet(charSet);
        this.content = null;
    }

}
