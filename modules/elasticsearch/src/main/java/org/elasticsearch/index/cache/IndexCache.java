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

package org.elasticsearch.index.cache;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.cache.field.data.none.NoneFieldDataCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexCache extends AbstractIndexComponent {

    private final FilterCache filterCache;

    private final FieldDataCache fieldDataCache;

    public IndexCache(Index index) {
        this(index, EMPTY_SETTINGS, new NoneFilterCache(index, EMPTY_SETTINGS), new NoneFieldDataCache(index, EMPTY_SETTINGS));
    }

    @Inject public IndexCache(Index index, @IndexSettings Settings indexSettings, FilterCache filterCache, FieldDataCache fieldDataCache) {
        super(index, indexSettings);
        this.filterCache = filterCache;
        this.fieldDataCache = fieldDataCache;
    }

    public FilterCache filter() {
        return filterCache;
    }

    public FieldDataCache fieldData() {
        return fieldDataCache;
    }

    public void clear(IndexReader reader) {
        filterCache.clear(reader);
        fieldDataCache.clear(reader);
    }

    public void clear() {
        filterCache.clear();
        fieldDataCache.clear();
    }

    public void clearUnreferenced() {
        filterCache.clearUnreferenced();
        fieldDataCache.clearUnreferenced();
    }
}
