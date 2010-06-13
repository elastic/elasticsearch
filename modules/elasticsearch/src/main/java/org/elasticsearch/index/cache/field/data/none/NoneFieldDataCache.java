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

package org.elasticsearch.index.cache.field.data.none;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataOptions;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class NoneFieldDataCache extends AbstractIndexComponent implements FieldDataCache {

    @Inject public NoneFieldDataCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        logger.debug("Using no field cache");
    }

    @Override public <T extends FieldData> T cache(Class<T> type, IndexReader reader, String fieldName, FieldDataOptions options) throws IOException {
        return FieldData.load(type, reader, fieldName, options);
    }

    @Override public FieldData cache(FieldData.Type type, IndexReader reader, String fieldName, FieldDataOptions options) throws IOException {
        return FieldData.load(type, reader, fieldName, options);
    }

    @Override public String type() {
        return "none";
    }

    @Override public void clear() {
    }

    @Override public void clear(IndexReader reader) {
    }

    @Override public void clearUnreferenced() {
    }

    @Override public void close() throws ElasticSearchException {
    }
}
