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

import java.io.*;

/**
 * This class is an adaptation of the Apache HttpClient implementation
 *
 * @link http://hc.apache.org/httpclient-3.x/
 */
public class FilePartSource implements PartSource {

    /**
     * File part file.
     */
    private File file = null;

    /**
     * File part file name.
     */
    private String fileName = null;

    /**
     * Constructor for FilePartSource.
     *
     * @param file the FilePart source File.
     * @throws java.io.FileNotFoundException if the file does not exist or
     *                                       cannot be read
     */
    public FilePartSource(File file) throws FileNotFoundException {
        this.file = file;
        if (file != null) {
            if (!file.isFile()) {
                throw new FileNotFoundException("File is not a normal file.");
            }
            if (!file.canRead()) {
                throw new FileNotFoundException("File is not readable.");
            }
            this.fileName = file.getName();
        }
    }

    /**
     * Constructor for FilePartSource.
     *
     * @param fileName the file name of the FilePart
     * @param file     the source File for the FilePart
     * @throws FileNotFoundException if the file does not exist or
     *                               cannot be read
     */
    public FilePartSource(String fileName, File file)
            throws FileNotFoundException {
        this(file);
        if (fileName != null) {
            this.fileName = fileName;
        }
    }

    /**
     * Return the length of the file
     *
     * @return the length of the file.
     * @see PartSource#getLength()
     */
    public long getLength() {
        if (this.file != null) {
            return this.file.length();
        } else {
            return 0;
        }
    }

    /**
     * Return the current filename
     *
     * @return the filename.
     * @see PartSource#getFileName()
     */
    public String getFileName() {
        return (fileName == null) ? "noname" : fileName;
    }

    /**
     * Return a new {@link java.io.FileInputStream} for the current filename.
     *
     * @return the new input stream.
     * @throws java.io.IOException If an IO problem occurs.
     * @see PartSource#createInputStream()
     */
    public InputStream createInputStream() throws IOException {
        if (this.file != null) {
            return new FileInputStream(this.file);
        } else {
            return new ByteArrayInputStream(new byte[]{});
        }
    }

}
