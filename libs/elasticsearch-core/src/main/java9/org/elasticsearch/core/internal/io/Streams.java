/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.core.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Simple utility methods for file and stream copying.
 * All copy methods close all affected streams when done.
 * <p>
 * Mainly for use within the framework,
 * but also useful for application code.
 */
public abstract class Streams {

    /**
     * Copy the contents of the given InputStream to the given OutputStream.
     * Closes both streams when done.
     *
     * @param in  the stream to copy from
     * @param out the stream to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static long copy(InputStream in, OutputStream out) throws IOException {
        boolean success = false;
        try {
            final long byteCount = in.transferTo(out);
            out.flush();
            success = true;
            return byteCount;
        } finally {
            if (success) {
                Exception ex = null;
                try {
                    in.close();
                } catch (IOException | RuntimeException e) {
                    ex = e;
                }

                try {
                    out.close();
                } catch (IOException | RuntimeException e) {
                    if (ex == null) {
                        ex = e;
                    } else {
                        ex.addSuppressed(e);
                    }
                }

                if (ex != null) {
                    if (ex instanceof IOException) {
                        throw (IOException) ex;
                    } else {
                        throw (RuntimeException) ex;
                    }
                }
            } else {
                try {
                    in.close();
                } catch (IOException | RuntimeException e) {
                    // empty
                }

                try {
                    out.close();
                } catch (IOException | RuntimeException e) {
                    // empty
                }
            }
        }

    }
}
