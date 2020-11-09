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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;

import static org.elasticsearch.index.mapper.IdFieldMapper.ID_FIELD_DATA_DEPRECATION_MESSAGE;
import static org.hamcrest.Matchers.containsString;

public class IdFieldMapperTests extends MapperServiceTestCase {

    public void testIncludeInObjectNotAllowed() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));

        Exception e = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(source(b -> b.field("_id", 1))));

        assertThat(e.getCause().getMessage(),
            containsString("Field [_id] is a metadata field and cannot be added inside a document"));
    }

    public void testDefaults() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument document = mapper.parse(source(b -> {}));
        IndexableField[] fields = document.rootDoc().getFields(IdFieldMapper.NAME);
        assertEquals(1, fields.length);
        assertEquals(IndexOptions.DOCS, fields[0].fieldType().indexOptions());
        assertTrue(fields[0].fieldType().stored());
        assertEquals(Uid.encodeId("1"), fields[0].binaryValue());
    }

    public void testEnableFieldData() throws IOException {

        boolean[] enabled = new boolean[1];

        MapperService mapperService = createMapperService(() -> enabled[0], mapping(b -> {}));
        IdFieldMapper.IdFieldType ft = (IdFieldMapper.IdFieldType) mapperService.fieldType("_id");

        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> ft.fielddataBuilder("test", () -> {
                throw new UnsupportedOperationException();
            }).build(null, null));
        assertThat(exc.getMessage(), containsString(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()));
        assertFalse(ft.isAggregatable());

        enabled[0] = true;
        ft.fielddataBuilder("test", () -> {
            throw new UnsupportedOperationException();
        }).build(null, null);
        assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        assertTrue(ft.isAggregatable());
    }

}
