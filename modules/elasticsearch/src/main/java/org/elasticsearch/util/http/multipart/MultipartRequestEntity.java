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

import org.elasticsearch.util.collect.ArrayListMultimap;
import org.elasticsearch.util.collect.Multimap;
import org.elasticsearch.util.logging.ESLogger;
import org.elasticsearch.util.logging.Loggers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class MultipartRequestEntity implements RequestEntity {

    private final static ESLogger log = Loggers.getLogger(MultipartRequestEntity.class);

    /**
     * The Content-Type for multipart/form-data.
     */
    private static final String MULTIPART_FORM_CONTENT_TYPE = "multipart/form-data";

    /**
     * The pool of ASCII chars to be used for generating a multipart boundary.
     */
    private static byte[] MULTIPART_CHARS = MultipartEncodingUtil.getAsciiBytes(
            "-_1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

    /**
     * Generates a random multipart boundary string.
     *
     * @return
     */
    private static byte[] generateMultipartBoundary() {
        Random rand = new Random();
        byte[] bytes = new byte[rand.nextInt(11) + 30]; // a random size from 30 to 40
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = MULTIPART_CHARS[rand.nextInt(MULTIPART_CHARS.length)];
        }
        return bytes;
    }

    /**
     * The MIME parts as set by the constructor
     */
    protected Part[] parts;

    private byte[] multipartBoundary;

    private Multimap<String, String> methodParams;

    /**
     * Creates a new multipart entity containing the given parts.
     *
     * @param parts        The parts to include.
     * @param methodParams The params of the HttpMethod using this entity.
     */
    public MultipartRequestEntity(Part[] parts, Multimap<String, String> methodParams) {
        if (parts == null) {
            throw new IllegalArgumentException("parts cannot be null");
        }
        if (methodParams == null) {
            methodParams = ArrayListMultimap.create();
        }
        this.parts = parts;
        this.methodParams = methodParams;
    }

    /**
     * Returns the MIME boundary string that is used to demarcate boundaries of
     * this part. The first call to this method will implicitly create a new
     * boundary string. To create a boundary string first the
     * HttpMethodParams.MULTIPART_BOUNDARY parameter is considered. Otherwise
     * a random one is generated.
     *
     * @return The boundary string of this entity in ASCII encoding.
     */
    protected byte[] getMultipartBoundary() {
        if (multipartBoundary == null) {
            String temp = methodParams.get("").isEmpty() ? null : methodParams.get("").iterator().next();
            if (temp != null) {
                multipartBoundary = MultipartEncodingUtil.getAsciiBytes(temp);
            } else {
                multipartBoundary = generateMultipartBoundary();
            }
        }
        return multipartBoundary;
    }

    /**
     * Returns <code>true</code> if all parts are repeatable, <code>false</code> otherwise.
     */
    public boolean isRepeatable() {
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].isRepeatable()) {
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.methods.RequestEntity#writeRequest(java.io.OutputStream)
     */

    public void writeRequest(OutputStream out) throws IOException {
        Part.sendParts(out, parts, getMultipartBoundary());
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.methods.RequestEntity#getContentLength()
     */

    public long getContentLength() {
        try {
            return Part.getLengthOfParts(parts, getMultipartBoundary());
        } catch (Exception e) {
            log.error("An exception occurred while getting the length of the parts", e);
            return 0;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.methods.RequestEntity#getContentType()
     */

    public String getContentType() {
        StringBuffer buffer = new StringBuffer(MULTIPART_FORM_CONTENT_TYPE);
        buffer.append("; boundary=");
        buffer.append(MultipartEncodingUtil.getAsciiString(getMultipartBoundary()));
        return buffer.toString();
    }

}
