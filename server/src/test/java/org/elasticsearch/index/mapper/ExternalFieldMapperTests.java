/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ExternalFieldMapperTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new ExternalMapperPlugin());
    }

    public void testExternalValues() throws Exception {

        DocumentMapper documentMapper = createDocumentMapper(fieldMapping(b -> b.field("type", "external")));

        ParsedDocument doc = documentMapper.parse(source(b -> b.field("field", "1234")));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField(ExternalMetadataMapper.FIELD_NAME).stringValue(), is(ExternalMetadataMapper.FIELD_VALUE));

    }

    public void testExternalValuesWithMultifield() throws Exception {

        DocumentMapper documentMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", ExternalMapperPlugin.EXTERNAL);
            b.startObject("fields");
            {
                b.startObject("text");
                {
                    b.field("type", "text");
                    b.field("store", true);
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = documentMapper.parse(source(b -> b.field("field", "1234")));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        IndexableField field = doc.rootDoc().getField("field.text");
        assertThat(field, notNullValue());
        assertThat(field.stringValue(), is("foo"));
    }
}
