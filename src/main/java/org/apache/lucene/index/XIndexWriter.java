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

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.lang.reflect.Method;

public final class XIndexWriter extends IndexWriter {

    private static final Method processEvents;


    static {
        // fix for https://issues.apache.org/jira/browse/LUCENE-5330
        assert Version.LUCENE_45.onOrAfter(org.elasticsearch.Version.CURRENT.luceneVersion) : "This should be fixed in LUCENE-4.6";
        try {
            processEvents = IndexWriter.class.getDeclaredMethod("processEvents", boolean.class, boolean.class);
            processEvents.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public XIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
        super(d, conf);
    }

    private void processEvents() {
        try {
            processEvents.invoke(this, false, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback() throws IOException {
        super.rollback();
        processEvents();
    }

    @Override
    public void close(boolean waitForMerges) throws IOException {
        super.close(waitForMerges);
        processEvents();
    }

    @Override
    DirectoryReader getReader(boolean applyAllDeletes) throws IOException {
        DirectoryReader reader = super.getReader(applyAllDeletes);
        processEvents();
        return reader;
    }

}