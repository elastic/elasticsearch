/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

public class TextFieldExactQueryTests extends MapperServiceTestCase {

    public void testBasic() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc": { "properties": { "text": { "type": "text" } } } }
            """);
        int docCount = randomIntBetween(500, 1000);
        MappedFieldType ft = mapperService.fieldType("text");

        withLuceneIndex(mapperService, iw -> {
            for (int outer = 0; outer < 3; outer++) {
                for (int i = 0; i < docCount; i++) {
                    int value = i;
                    ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("text", English.intToEnglish(value))));
                    iw.addDocument(doc.rootDoc());
                }
            }
        }, ir -> {
            IndexSearcher searcher = new IndexSearcher(ir);
            SearchExecutionContext sec = createSearchExecutionContext(mapperService, searcher);
            for (int i = 0; i < 20; i++) {
                String value = English.intToEnglish(i);
                Query q = ft.exactQuery(value, sec);
                assertEquals(3, searcher.count(q));
            }
        });

    }
}
