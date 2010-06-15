/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author kimchy (shay.banon)
 */
public abstract class IndexWriters {

    private static ESLogger logger = Loggers.getLogger(IndexWriters.class);

    private static Field docWriterField;

    private static Method docWriterGetRAMUsed;

    private static final boolean docWriterReflection;

    static {
        boolean docWriterReflectionX = false;
        try {
            docWriterField = IndexWriter.class.getDeclaredField("docWriter");
            docWriterField.setAccessible(true);
            Class docWriter = IndexWriters.class.getClassLoader().loadClass("org.apache.lucene.index.DocumentsWriter");
            docWriterGetRAMUsed = docWriter.getDeclaredMethod("getRAMUsed");
            docWriterGetRAMUsed.setAccessible(true);
            docWriterReflectionX = true;
        } catch (Exception e) {
            logger.warn("Failed to get doc writer field", e);
        }
        docWriterReflection = docWriterReflectionX;
    }

    public static long estimateRamSize(IndexWriter indexWriter) throws Exception {
        if (!docWriterReflection) {
            return -1;
        }
        Object docWriter = docWriterField.get(indexWriter);
        return (Long) docWriterGetRAMUsed.invoke(docWriter);
    }

    private IndexWriters() {

    }
}
