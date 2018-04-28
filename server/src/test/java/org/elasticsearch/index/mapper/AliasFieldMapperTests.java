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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class AliasFieldMapperTests extends FieldMapperTestCase {

    private static final String ALIAS1 = "alias1";
    private static final String NESTED2 = "nested2";
    private static final String TEXT3 = "text3";
    private static final String TEXT4 = "text4";
    private static final String TEXT5 = "text5";

    private static final String ALIAS_TO_PATH = fieldPath(TEXT4);
    private static final String ALIAS1_FIELD_MAPPING = aliasFieldMapping(TEXT4);

    private static final String VALUE4 = "value4";
    private static final String VALUE5 = "value5";

    // MAPPING VALIDATION...

    public void testPathMissingFails() throws Exception {
        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(TYPE)
            .startObject("properties")
            .startObject(ALIAS1)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .endObject()
            .endObject()
            .endObject()
            .endObject());
        this.documentMapperFails(mapping,
            MapperParsingException.class,
            AliasFieldMapper.propertyPathMissing());
    }

    public void testPathEmptyFails() throws Exception {
        this.documentMapperFails(this.mapping(""),
            MapperParsingException.class,
            AliasFieldMapper.propertyPathEmptyOrBlank());
    }

    public void testPathBlankFails() throws Exception {
        this.documentMapperFails(this.mapping("   "),
            MapperParsingException.class,
            AliasFieldMapper.propertyPathEmptyOrBlank());
    }

    public void testPathUnknownFails() throws Exception {
        final String path = "path.to.nothing";
        this.documentMapperFails(this.mapping(path),
            MapperException.class,
            AliasFieldMapper.propertyPathUnknown(path));
    }

    public void testPathSelfFails() throws Exception {
        this.documentMapperFails(this.mapping(ALIAS1),
            MapperException.class,
            AliasFieldMapper.propertyPathToSelf(ALIAS1));
    }

    public void testPathToAnotherAliasFails() throws Exception {
        final String alias2 = "alias2";

        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(TYPE)
            .startObject("properties")
            .startObject(ALIAS1)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, alias2)
            .endObject()
            .startObject(alias2)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, TEXT3)
            .endObject()
            .startObject(TEXT3).field("type", "text").endObject()
            .endObject()
            .endObject()
            .endObject());
        this.documentMapperFails(mapping,
            MapperException.class,
            AliasFieldMapper.propertyPathToAnotherAlias(alias2));
    }

    public void testPathToNestedAliasFails() throws Exception {
        final String alias3 = "alias3";
        final String pathTo3 = fieldPath(NESTED2, alias3);

        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(TYPE)
            .startObject("properties")
            .startObject(ALIAS1)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, pathTo3)
            .endObject()
            .startObject(NESTED2)
            .field("type", "nested")
            .startObject("properties")
            .startObject(alias3)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, TEXT4)
            .endObject()
            .endObject()
            .endObject()
            .startObject(TEXT4).field("type", "text").endObject()
            .endObject()
            .endObject()
            .endObject());
        this.documentMapperFails(mapping,
            MapperException.class,
            AliasFieldMapper.propertyPathToAnotherAlias(pathTo3));
    }

    public void testPathToNestedNestedAliasFails() throws Exception {
        final String nested3 = "nested3";
        final String alias4 = "alias4";
        final String pathTo4 = fieldPath(NESTED2, nested3, alias4);
        final String text5 = "text5";

        final String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .startObject(TYPE)
            .startObject("properties")
            .startObject(ALIAS1)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, pathTo4)
            .endObject()
            .startObject(NESTED2)
            .field("type", "nested")
            .startObject("properties")
            .startObject(nested3)
            .field("type", "nested")
            .startObject("properties")
            .startObject(alias4)
            .field("type", AliasFieldMapper.CONTENT_TYPE)
            .field(AliasFieldMapper.TypeParser.PATH, text5)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .startObject(text5).field("type", "text").endObject()
            .endObject()
            .endObject()
            .endObject());
        this.documentMapperFails(mapping,
            MapperException.class,
            AliasFieldMapper.propertyPathToAnotherAlias(pathTo4));
    }

    public void testPathToObjectFails() throws Exception {
        this.documentMapperFails(this.mapping(NESTED2),
            MapperException.class,
            AliasFieldMapper.propertyPathNotField(NESTED2));
    }

    public void testMappingValid() throws Exception {
        this.documentMapper();
    }

    public void testSerialization() throws IOException {
        this.checkFieldMapping(this.documentMapper(),
            ALIAS1,
            aliasFieldMapping(TEXT4));
    }

    // MERGE MAPPING...

    public void testMappingMergeSameMapping() throws Exception {
        final String mapping = this.mapping();
        this.documentMapper(mapping);
        this.mergeMapping(mapping);

        this.checkFieldMapping(this.documentMapperFromIndexService(),
            ALIAS1,
            ALIAS1_FIELD_MAPPING);
    }

    public void testMappingMergeInvalidFails() throws Exception {
        this.documentMapper(this.mapping(TEXT4));

        this.mergeMappingFails(this.mapping(ALIAS1),
            MapperException.class,
            AliasFieldMapper.propertyPathToSelf(ALIAS1));
    }

    public void testMappingMergeUpdateDifferentMapping() throws Exception {
        this.documentMapper(this.mapping(TEXT4));
        this.mergeMapping(this.mapping(TEXT5));

        this.checkFieldMapping(this.documentMapperFromIndexService(),
            ALIAS1,
            aliasFieldMapping(TEXT5));
    }

    // INDEXING.....

    public void testMappingParseAliasFieldFails() throws Exception {
        final BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field(ALIAS1, VALUE4)
                .endObject());

        this.mappingParseFails(source,
            MapperParsingException.class,
            AliasFieldMapper.indexingUnsupported(ALIAS1, ALIAS_TO_PATH));
    }

    public void testMappingParseWithoutAliasField() throws Exception {
        final List<ParseContext.Document> documents = this.mappingParse(this.source(VALUE4, VALUE5));
        this.checkDocumentCount(1, documents);

        final ParseContext.Document root = documents.get(0);
        this.checkField(root, TEXT4, VALUE4);
        this.checkField(root, TEXT5, VALUE5);
    }

    @Override
    protected String mapping() throws IOException {
        return this.mapping(ALIAS_TO_PATH);
    }

    private String mapping(final String aliasToPath) throws IOException {
        return Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject(TYPE)
                    .startObject("properties")
                        .startObject(ALIAS1)
                            .field("type", AliasFieldMapper.CONTENT_TYPE)
                            .field(AliasFieldMapper.TypeParser.PATH, aliasToPath)
                            //.field("type", "text")
                        .endObject()
                        .startObject(NESTED2)
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject(TEXT3).field("type", "text").field("index", true).endObject()
                            .endObject()
                        .endObject()
                        .startObject(TEXT4).field("type", "text").field("index", true).endObject()
                        .startObject(TEXT5).field("type", "text").field("index", true).endObject()
                    .endObject()
                .endObject()
            .endObject());
    }

    private static String aliasFieldMapping(final String pathTo) {
        return "{\"alias1\":{\"type\":\"" + AliasFieldMapper.CONTENT_TYPE + "\",\"" +
            AliasFieldMapper.TypeParser.PATH + "\":\"" + pathTo + "\"}}";
    }

    private BytesReference source(final String value4, final String value5) throws IOException {
        return BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .field(TEXT4, value4)
                    .field(TEXT5, value5)
                .endObject());
    }
}
