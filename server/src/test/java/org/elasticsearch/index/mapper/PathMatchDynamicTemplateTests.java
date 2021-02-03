/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

public class PathMatchDynamicTemplateTests extends ESSingleNodeTestCase {
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/pathmatch/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setType("person").setSource(mapping, XContentType.JSON).get();

        MapperService mapperService = index.mapperService();

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/pathmatch/test-data.json");
        ParsedDocument parsedDoc = mapperService.documentMapper().parse(
            new SourceToParse("test", "person", "1", new BytesArray(json), XContentType.JSON));
        client().admin().indices().preparePutMapping("test").setType("person")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("top_level"));
        assertThat(f.fieldType().stored(), equalTo(false));

        assertThat(mapperService.fieldType("name").isStored(), equalTo(false));

        f = doc.getField("obj1.name");
        assertThat(f.name(), equalTo("obj1.name"));
        assertThat(f.fieldType().stored(), equalTo(true));

        assertThat(mapperService.fieldType("obj1.name").isStored(), equalTo(true));

        f = doc.getField("obj1.obj2.name");
        assertThat(f.name(), equalTo("obj1.obj2.name"));
        assertThat(f.fieldType().stored(), equalTo(false));

        assertThat(mapperService.fieldType("obj1.obj2.name").isStored(), equalTo(false));

        // verify more complex path_match expressions
        assertNotNull(mapperService.fieldType("obj3.obj4.prop1").getTextSearchInfo());
    }
}
