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

import java.io.UnsupportedEncodingException;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class MultipartEncodingUtil {

    /**
     * Log object for this class.
     */
    private final static ESLogger LOG = Loggers.getLogger(MultipartEncodingUtil.class);

    public static byte[] getAsciiBytes(String data) {
        try {
            return data.getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getAsciiString(final byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Parameter may not be null");
        }

        try {
            return new String(data, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] getBytes(final String data, String charset) {

        if (data == null) {
            throw new IllegalArgumentException("data may not be null");
        }

        if (charset == null || charset.length() == 0) {
            throw new IllegalArgumentException("charset may not be null or empty");
        }

        try {
            return data.getBytes(charset);
        } catch (UnsupportedEncodingException e) {

            if (LOG.isDebugEnabled()) {
                LOG.warn("Unsupported encoding: " + charset + ". System encoding used.");
            }

            return data.getBytes();
        }
    }
}
