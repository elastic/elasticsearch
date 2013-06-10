/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.dump;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.Streams;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 */
public abstract class AbstractDump implements Dump {

    private final long timestamp;

    private final String cause;

    private final Map<String, Object> context;

    private final ArrayList<File> files = new ArrayList<File>();

    protected AbstractDump(long timestamp, String cause, @Nullable Map<String, Object> context) {
        this.timestamp = timestamp;
        this.cause = cause;
        if (context == null) {
            context = ImmutableMap.of();
        }
        this.context = context;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public Map<String, Object> context() {
        return this.context;
    }

    @Override
    public String cause() {
        return cause;
    }

    @Override
    public File[] files() {
        return files.toArray(new File[files.size()]);
    }

    @Override
    public File createFile(String name) throws DumpException {
        File file = doCreateFile(name);
        files.add(file);
        return file;
    }

    protected abstract File doCreateFile(String name) throws DumpException;

    @Override
    public OutputStream createFileOutputStream(String name) throws DumpException {
        try {
            return new FileOutputStream(createFile(name));
        } catch (FileNotFoundException e) {
            throw new DumpException("Failed to create file [" + name + "]", e);
        }
    }

    @Override
    public Writer createFileWriter(String name) throws DumpException {
        return new OutputStreamWriter(createFileOutputStream(name), Charsets.UTF_8);
    }
}
