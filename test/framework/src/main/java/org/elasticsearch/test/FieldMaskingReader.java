/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldFilterLeafReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;

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
