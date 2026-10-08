/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.TemporalityAttribute;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class FetchabilityTests extends ESTestCase {

    public void testMappedFields() {
        assertFetchable(field(new EsField("f", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        assertFetchable(field(new EsField("f", DataType.GEO_POINT, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
        assertFetchable(field(new KeywordEsField("f", Map.of(), true, 32766, false, false, EsField.TimeSeriesFieldType.NONE)));
        assertFetchable(field(new TextEsField("f", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE)));
        assertFetchable(field(DateEsField.dateEsField("f", Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
    }

    public void testUnionTypesAreFetchable() {
        assertFetchable(field(new MultiTypeEsField("f", DataType.DATE_NANOS, true, Map.of(), EsField.TimeSeriesFieldType.NONE, null)));
    }

    public void testFieldsWithoutAMappingAreNotFetchable() {
        assertNotFetchable(field(new PotentiallyUnmappedKeywordEsField("f")));
        assertNotFetchable(field(new MissingEsField("f")));
        assertNotFetchable(field(new UnsupportedEsField("f", List.of("geo_shape_custom"))));
        assertNotFetchable(field(new EsField("f", DataType.UNSUPPORTED, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
    }

    public void testDocumentIdentityIsNotFetchable() {
        assertNotFetchable(field(EsQueryExec.DOC_ID_FIELD));
        assertNotFetchable(new MetadataAttribute(Source.EMPTY, MetadataAttribute.DOC, DataType.DOC_DATA_TYPE, false));
    }

    public void testMetadata() {
        for (String name : Set.of("_version", "_index", "_id", "_ignored", "_source", "_index_mode", "_tsid", "_size")) {
            assertFetchable(metadata(name));
        }
        // computed by the query, not stored with the document
        assertNotFetchable(metadata(MetadataAttribute.SCORE));
        // replaced by constants during logical optimization
        assertNotFetchable(metadata(MetadataAttribute.RELATION_CLASS));
        assertNotFetchable(metadata(MetadataAttribute.RELATION_NAME));
    }

    public void testComputedAttributesAreNotFetchable() {
        assertNotFetchable(new ReferenceAttribute(Source.EMPTY, null, "r", DataType.LONG));
        assertNotFetchable(new TemporalityAttribute(Source.EMPTY));
        assertNotFetchable(new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of()));
    }

    private static FieldAttribute field(EsField field) {
        return new FieldAttribute(Source.EMPTY, field.getName(), field);
    }

    private static MetadataAttribute metadata(String name) {
        DataType type = MetadataAttribute.dataType(name);
        return new MetadataAttribute(Source.EMPTY, name, type == null ? DataType.KEYWORD : type, false);
    }

    private static void assertFetchable(Attribute attribute) {
        assertTrue(attribute + " should be fetchable", Fetchability.isFetchable(attribute));
    }

    private static void assertNotFetchable(Attribute attribute) {
        assertFalse(attribute + " should not be fetchable", Fetchability.isFetchable(attribute));
    }
}
