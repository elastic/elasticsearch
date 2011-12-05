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
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.lang.reflect.Field;

/**
 * @author kimchy (shay.banon)
 */
public abstract class IndexWriters {

    private static ESLogger logger = Loggers.getLogger(IndexWriters.class);

    private static Field rollbackSegmentInfosField;

    private static final boolean docWriterReflection;

    static {
        boolean docWriterReflectionX = false;
        try {
            rollbackSegmentInfosField = IndexWriter.class.getDeclaredField("rollbackSegmentInfos");
            rollbackSegmentInfosField.setAccessible(true);

            docWriterReflectionX = true;
        } catch (Exception e) {
            logger.warn("Failed to doc writer fields", e);
        }
        docWriterReflection = docWriterReflectionX;
    }

    public static SegmentInfos rollbackSegmentInfos(IndexWriter writer) throws Exception {
        return (SegmentInfos) rollbackSegmentInfosField.get(writer);
    }

    private IndexWriters() {

    }
}
