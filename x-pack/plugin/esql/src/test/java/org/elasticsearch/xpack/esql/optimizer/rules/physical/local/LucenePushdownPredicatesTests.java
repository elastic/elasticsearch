/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;

import java.util.Map;

import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.hamcrest.Matchers.equalTo;

/**
 * A field under a nested object keeps its values in the child documents, so nothing over it is pushed to Lucene, even where
 * field caps or the shards report it as indexed with doc values.
 */
public class LucenePushdownPredicatesTests extends ESTestCase {

    public void testFieldUnderNestedObjectIsNotPushable() {
        for (LucenePushdownPredicates predicates : new LucenePushdownPredicates[] {
            LucenePushdownPredicates.DEFAULT,
            LucenePushdownPredicates.DEFAULT.bestEffort() }) {
            assertThat(predicates.isPushableFieldAttribute(keyword(false)), equalTo(true));
            assertThat(predicates.isPushableFieldAttribute(keyword(true)), equalTo(false));
            assertThat(predicates.isPushableValueAttribute(keyword(true)), equalTo(false));
        }
    }

    public void testTextUnderNestedObjectIsNotPushable() {
        assertThat(LucenePushdownPredicates.isPushableTextFieldAttribute(text(false)), equalTo(true));
        assertThat(LucenePushdownPredicates.isPushableTextFieldAttribute(text(true)), equalTo(false));
    }

    public void testSpatialUnderNestedObjectIsNotPushable() {
        assertThat(intersects(false).translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.YES));
        assertThat(intersects(true).translatable(LucenePushdownPredicates.DEFAULT), equalTo(TranslationAware.Translatable.NO));
    }

    /** An aggregatable keyword, as field caps reports one with doc values. */
    private static FieldAttribute keyword(boolean underNested) {
        return new FieldAttribute(
            Source.EMPTY,
            "items.sku",
            new KeywordEsField(
                "items.sku",
                Map.of(),
                true,
                Short.MAX_VALUE,
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                false,
                underNested
            )
        );
    }

    private static FieldAttribute text(boolean underNested) {
        return new FieldAttribute(
            Source.EMPTY,
            "items.note",
            new TextEsField(
                "items.note",
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                null,
                TextEsField.DEFAULT_POSITION_INCREMENT_GAP,
                TextEsField.UnknownAnalyzer.NOT_REPORTED,
                null,
                underNested
            )
        );
    }

    /** An aggregatable geo_point, as field caps reports one that is indexed with doc values. */
    private static SpatialIntersects intersects(boolean underNested) {
        FieldAttribute location = new FieldAttribute(
            Source.EMPTY,
            "items.location",
            new EsField(
                "items.location",
                DataType.GEO_POINT,
                Map.of(),
                true,
                false,
                EsField.TimeSeriesFieldType.NONE,
                false,
                false,
                underNested
            )
        );
        Literal point = new Literal(Source.EMPTY, GEO.wktToWkb("POINT(0 0)"), DataType.GEO_POINT);
        return new SpatialIntersects(Source.EMPTY, location, point);
    }
}
