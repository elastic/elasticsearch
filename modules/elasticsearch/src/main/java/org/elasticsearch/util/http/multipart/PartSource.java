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
import java.io.InputStream;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public interface PartSource {

    /**
     * Gets the number of bytes contained in this source.
     *
     * @return a value >= 0
     */
    long getLength();

    /**
     * Gets the name of the file this source represents.
     *
     * @return the fileName used for posting a MultiPart file part
     */
    String getFileName();

    /**
     * Gets a new InputStream for reading this source.  This method can be
     * called more than once and should therefore return a new stream every
     * time.
     *
     * @return a new InputStream
     * @throws java.io.IOException if an error occurs when creating the InputStream
     */
    InputStream createInputStream() throws IOException;

}
