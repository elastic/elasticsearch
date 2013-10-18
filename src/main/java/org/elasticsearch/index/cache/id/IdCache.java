/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.id;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.component.CloseableComponent;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.service.IndexService;

import java.io.IOException;
import java.util.List;

/**
 * This id cache contains only the ids of parent documents, loaded via the uid or parent field.
 * This name IdCache is misleading, parentIdCache would be a better name.
 */
public interface IdCache extends IndexComponent, CloseableComponent, Iterable<IdReaderCache> {

    // we need to "inject" the index service to not create cyclic dep
    void setIndexService(IndexService indexService);

    void clear();

    void clear(Object coreCacheKey);

    void refresh(List<AtomicReaderContext> readers) throws IOException;

    IdReaderCache reader(AtomicReader reader);
}
