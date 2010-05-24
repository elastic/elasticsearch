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
 *
 */
package org.elasticsearch.util.http.client;

public class ByteArrayPart implements Part {
    private String name;
    private String fileName;
    private byte[] data;
    private String mimeType;
    private String charSet;

    public ByteArrayPart(String name, String fileName, byte[] data, String mimeType, String charSet) {
        this.name = name;
        this.fileName = fileName;
        this.data = data;
        this.mimeType = mimeType;
        this.charSet = charSet;
    }

    public String getName() {
        return name;
    }

    public String getFileName() {
        return fileName;
    }

    public byte[] getData() {
        return data;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getCharSet() {
        return charSet;
    }
}
