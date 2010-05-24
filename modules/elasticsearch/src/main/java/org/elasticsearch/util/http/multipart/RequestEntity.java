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

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public interface RequestEntity {

    /**
     * Tests if {@link #writeRequest(java.io.OutputStream)} can be called more than once.
     *
     * @return <tt>true</tt> if the entity can be written to {@link java.io.OutputStream} more than once,
     *         <tt>false</tt> otherwise.
     */
    boolean isRepeatable();

    /**
     * Writes the request entity to the given stream.
     *
     * @param out
     * @throws java.io.IOException
     */
    void writeRequest(OutputStream out) throws IOException;

    /**
     * Gets the request entity's length. This method should return a non-negative value if the content
     * length is known or a negative value if it is not. In the latter case the
     * EntityEnclosingMethod will use chunk encoding to
     * transmit the request entity.
     *
     * @return a non-negative value when content length is known or a negative value when content length
     *         is not known
     */
    long getContentLength();

    /**
     * Gets the entity's content type.  This content type will be used as the value for the
     * "Content-Type" header.
     *
     * @return the entity's content type
     */
    String getContentType();

}
