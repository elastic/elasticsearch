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

package org.elasticsearch.gradle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An outputstream to a File that is lazily opened on the first write.
 */
class LazyFileOutputStream extends OutputStream {
    private OutputStream delegate;

    LazyFileOutputStream(File file) {
        // use an initial dummy delegate to avoid doing a conditional on every write
        this.delegate = new OutputStream() {
            private void bootstrap() throws IOException {
                file.getParentFile().mkdirs();
                delegate = new FileOutputStream(file);
            }

            @Override
            public void write(int b) throws IOException {
                bootstrap();
                delegate.write(b);
            }

            @Override
            public void write(byte b[], int off, int len) throws IOException {
                bootstrap();
                delegate.write(b, off, len);
            }
        };
    }

    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        delegate.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
