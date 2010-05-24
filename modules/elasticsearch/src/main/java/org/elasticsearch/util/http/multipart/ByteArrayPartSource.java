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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class ByteArrayPartSource implements PartSource {

    /**
     * Name of the source file.
     */
    private String fileName;

    /**
     * Byte array of the source file.
     */
    private byte[] bytes;

    /**
     * Constructor for ByteArrayPartSource.
     *
     * @param fileName the name of the file these bytes represent
     * @param bytes    the content of this part
     */
    public ByteArrayPartSource(String fileName, byte[] bytes) {

        this.fileName = fileName;
        this.bytes = bytes;

    }

    /**
     * @see PartSource#getLength()
     */
    public long getLength() {
        return bytes.length;
    }

    /**
     * @see PartSource#getFileName()
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @see PartSource#createInputStream()
     */
    public InputStream createInputStream() throws IOException {
        return new ByteArrayInputStream(bytes);
    }

}
