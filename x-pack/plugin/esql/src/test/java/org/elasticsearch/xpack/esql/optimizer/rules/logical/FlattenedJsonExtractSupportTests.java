/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FlattenedEsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;

import static org.hamcrest.Matchers.instanceOf;

public class FlattenedJsonExtractSupportTests extends ESTestCase {

    public void testValidDottedPath() {
        assertNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("host.name"));
        assertTrue(FlattenedJsonExtractSupport.isKeyedFlattenedSubfieldPath("a.b.c"));
    }

    public void testRejectEmptyOrMalformedPath() {
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath(""));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath(".a"));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a."));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a..b"));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a*b"));
    }

    public void testRejectJsonPathShapes() {
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a[0]"));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("$a"));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a b"));
        assertNotNull(FlattenedJsonExtractSupport.validateKeyedFlattenedPath("a\"b"));
    }

    public void testSyntheticKeyedSubfield() {
        Source src = Source.EMPTY;
        FieldAttribute root = new FieldAttribute(src, "resource.attributes", new FlattenedEsField("resource.attributes", true));
        FieldAttribute sub = FlattenedJsonExtractSupport.syntheticKeyedSubfield(src, root, "host.name");
        assertTrue(sub.synthetic());
        assertEquals("resource.attributes.host.name", sub.name());
        assertThat(sub.field(), instanceOf(KeywordEsField.class));
    }
}
