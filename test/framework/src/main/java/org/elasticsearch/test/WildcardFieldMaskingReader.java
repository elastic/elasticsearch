/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.index.FieldFilterLeafReader;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class WildcardFieldMaskingReader extends FilterDirectoryReader {
    private final Set<String> patterns;

    public WildcardFieldMaskingReader(String pattern, DirectoryReader in) throws IOException {
        this(Set.of(pattern), in);
    }

    public WildcardFieldMaskingReader(Set<String> patterns, DirectoryReader in) throws IOException {
        super(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                var matcher = XContentMapValues.compileAutomaton(
                    patterns.toArray(String[]::new),
                    new CharacterRunAutomaton(Automata.makeAnyString())
                );
                Set<String> fields = new TreeSet<>();

                for (var fieldInfo : reader.getFieldInfos()) {
                    String fieldName = fieldInfo.name;
                    if (matcher.run(fieldName)) {
                        fields.add(fieldName);
                    }
                }

                return new FilterLeafReader(new FieldFilterLeafReader(reader, fields, true)) {
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
        this.patterns = patterns;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new WildcardFieldMaskingReader(patterns, in);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
