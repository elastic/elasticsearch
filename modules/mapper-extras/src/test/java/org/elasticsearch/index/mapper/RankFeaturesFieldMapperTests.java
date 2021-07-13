/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.plugins.Plugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RankFeaturesFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("ten", 10, "twenty", 20);
    }

    @Override
    protected void assertExistsQuery(MapperService mapperService) {
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> super.assertExistsQuery(mapperService));
        assertEquals("[rank_features] fields do not support [exists] queries", iae.getMessage());
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MapperExtrasPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "rank_features");
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("positive_score_impact", b -> b.field("positive_score_impact", false));
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(this::writeField));

        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = null;
        FeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("ten")) {
                featureField1 = (FeatureField)field;
            } else if (field.stringValue().equals("twenty")) {
                featureField2 = (FeatureField)field;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        int freq1 = RankFeatureFieldMapperTests.getFrequency(featureField1.tokenStream(null, null));
        int freq2 = RankFeatureFieldMapperTests.getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 < freq2);
    }

    public void testNegativeScoreImpact() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(b -> b.field("type", "rank_features").field("positive_score_impact", false))
        );

        ParsedDocument doc1 = mapper.parse(source(this::writeField));

        IndexableField[] fields = doc1.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertThat(fields[0], Matchers.instanceOf(FeatureField.class));
        FeatureField featureField1 = null;
        FeatureField featureField2 = null;
        for (IndexableField field : fields) {
            if (field.stringValue().equals("ten")) {
                featureField1 = (FeatureField)field;
            } else if (field.stringValue().equals("twenty")) {
                featureField2 = (FeatureField)field;
            } else {
                throw new UnsupportedOperationException();
            }
        }

        int freq1 = RankFeatureFieldMapperTests.getFrequency(featureField1.tokenStream(null, null));
        int freq2 = RankFeatureFieldMapperTests.getFrequency(featureField2.tokenStream(null, null));
        assertTrue(freq1 > freq2);
    }

    public void testRejectMultiValuedFields() throws MapperParsingException, IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("field").field("type", "rank_features").endObject();
            b.startObject("foo").startObject("properties");
            {
                b.startObject("field").field("type", "rank_features").endObject();
            }
            b.endObject().endObject();
        }));

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").field("foo", Arrays.asList(10, 20)).endObject()))
        );
        assertEquals("[rank_features] fields take hashes that map a feature to a strictly positive float, but got unexpected token " +
                "START_ARRAY", e.getCause().getMessage());

        e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().startObject("field").field("bar", 10).endObject().endObject();
                b.startObject().startObject("field").field("bar", 20).endObject().endObject();
            }
            b.endArray();
        })));
        assertEquals("[rank_features] fields do not support indexing multiple values for the same rank feature [foo.field.bar] in " +
                "the same document", e.getCause().getMessage());
    }

    public void testCannotBeUsedInMultifields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "keyword");
            b.startObject("fields");
            b.startObject("feature");
            b.field("type", "rank_features");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [feature] of type [rank_features] can't be used in multifields"));
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    @Override
    protected boolean allowsNullValues() {
        return false;       // TODO should this allow null values?
    }
}
