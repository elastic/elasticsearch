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
package org.elasticsearch.test;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldFilterLeafReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.IndexReader.CacheHelper;

import java.io.IOException;
import java.util.Collections;

public class FieldMaskingReader extends FilterDirectoryReader {
    private final String field;
    public FieldMaskingReader(String field, DirectoryReader in) throws IOException {
        super(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new FilterLeafReader(new FieldFilterLeafReader(reader, Collections.singleton(field), true)) {

                    // FieldFilterLeafReader does not forward cache helpers
                    // since it considers it is illegal because of the fact
                    // that it changes the content of the index. However we
                    // want this behavior for tests, and security plugins
                    // are careful to only use the cache when it's valid

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return reader.getReaderCacheHelper();
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return reader.getCoreCacheHelper();
                    }
                };
            }
        });
        this.field = field;

    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new FieldMaskingReader(field, in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}