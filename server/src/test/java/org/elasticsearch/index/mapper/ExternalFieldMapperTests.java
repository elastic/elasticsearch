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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.closeTo;
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

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        GeoPoint point = new GeoPoint().resetFromIndexableField(doc.rootDoc().getField("field.point"));
        assertThat(point.lat(), closeTo(42.0, 1e-5));
        assertThat(point.lon(), closeTo(51.0, 1e-5));

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

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
                    b.startObject("fields");
                    {
                        b.startObject("raw");
                        {
                            b.field("type", "keyword");
                            b.field("store", true);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = documentMapper.parse(source(b -> b.field("field", "1234")));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        GeoPoint point = new GeoPoint().resetFromIndexableField(doc.rootDoc().getField("field.point"));
        assertThat(point.lat(), closeTo(42.0, 1E-5));
        assertThat(point.lon(), closeTo(51.0, 1E-5));

        IndexableField shape = doc.rootDoc().getField("field.shape");
        assertThat(shape, notNullValue());

        IndexableField field = doc.rootDoc().getField("field.text");
        assertThat(field, notNullValue());
        assertThat(field.stringValue(), is("foo"));

        IndexableField raw = doc.rootDoc().getField("field.text.raw");

        assertThat(raw, notNullValue());
        assertThat(raw.binaryValue(), is(new BytesRef("foo")));

        assertWarnings("At least one multi-field, [text], was " +
            "encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated and will " +
            "no longer be supported in 8.0. To resolve the issue, all instances of [fields] that occur within a [fields] block " +
            "should be removed from the mappings, either by flattening the chained [fields] blocks into a single level, or " +
            "switching to [copy_to] if appropriate.");
    }

    public void testExternalValuesWithMultifieldTwoLevels() throws Exception {

        DocumentMapper documentMapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", ExternalMapperPlugin.EXTERNAL);
            b.startObject("fields");
            {
                b.startObject("text");
                {
                    b.field("type", "text");
                    b.startObject("fields");
                    {
                        b.startObject("generated").field("type", ExternalMapperPlugin.EXTERNAL_BIS).endObject();
                        b.startObject("raw").field("type", "text").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject("raw").field("type", "text").endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = documentMapper.parse(source(b -> b.field("field", "1234")));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.text"), notNullValue());
        assertThat(doc.rootDoc().getField("field.text").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.text.generated.generated"), notNullValue());
        assertThat(doc.rootDoc().getField("field.text.generated.generated").stringValue(), is("bar"));

        assertThat(doc.rootDoc().getField("field.text.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.text.raw").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.raw").stringValue(), is("foo"));

        assertWarnings("At least one multi-field, [text], was " +
            "encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated and will " +
            "no longer be supported in 8.0. To resolve the issue, all instances of [fields] that occur within a [fields] block " +
            "should be removed from the mappings, either by flattening the chained [fields] blocks into a single level, or " +
            "switching to [copy_to] if appropriate.");
    }
}
