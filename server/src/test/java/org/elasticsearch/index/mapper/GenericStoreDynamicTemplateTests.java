/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

public class GenericStoreDynamicTemplateTests extends MapperServiceTestCase {
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-mapping.json");
        MapperService mapperService = createMapperService(mapping);

        String json = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/genericstore/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(source(json));
        merge(mapperService, dynamicMapping(parsedDoc.dynamicMappingsUpdate()));
        LuceneDocument doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        assertTrue(mapperService.fieldType("name").isStored());

        boolean stored = false;
        for (IndexableField field : doc.getFields("age")) {
            stored |= field.fieldType().stored();
        }
        assertTrue(stored);
    }
}
