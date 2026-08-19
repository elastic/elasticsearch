/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class ViewMetadataFieldRewriterTests extends ESTestCase {

    public static final String VIEW_NAME = "my_view";
    private static final LogicalPlan BODY = new UnresolvedRelation(
        Source.EMPTY,
        new IndexPattern(Source.EMPTY, "languages"),
        false,
        List.of(),
        IndexMode.STANDARD,
        null,
        "FROM"
    );

    public void testNoMetadata_returnsSameInstance() {
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite(VIEW_NAME, BODY, List.of());
        assertThat(result, sameInstance(BODY));
    }

    public void testIndexMetadata_introducesAliasToViewName() {
        MetadataAttribute indexField = new MetadataAttribute(Source.EMPTY, MetadataAttribute.INDEX, DataType.KEYWORD, false);
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite(VIEW_NAME, BODY, List.of(indexField));

        assertThat(result, instanceOf(Eval.class));
        Eval eval = (Eval) result;
        assertThat(eval.child(), sameInstance(BODY));
        assertThat(eval.fields(), hasSize(1));
        Alias alias = eval.fields().getFirst();
        assertEquals(MetadataAttribute.INDEX, alias.name());
        assertEquals(VIEW_NAME, alias.child().toString());
    }

    public void testIdMetadata_introducesConcatOfViewNameAndIdAttribute() {
        MetadataAttribute idField = new MetadataAttribute(Source.EMPTY, IdFieldMapper.NAME, DataType.KEYWORD, false);
        LogicalPlan result = ViewMetadataFieldRewriter.rewrite(VIEW_NAME, BODY, List.of(idField));

        assertThat(result, instanceOf(Eval.class));
        Eval eval = (Eval) result;

        assertThat(eval.child(), instanceOf(UnresolvedRelation.class));
        UnresolvedRelation innerRelation = (UnresolvedRelation) eval.child();
        assertTrue("The relation inside the view body had a _id metadataField injected",
            innerRelation.metadataFields().stream().anyMatch(f -> IdFieldMapper.NAME.equals(f.name())));

        assertThat(eval.fields(), hasSize(1));
        Alias alias = eval.fields().getFirst();
        assertEquals(IdFieldMapper.NAME, alias.name());

        assertThat("A Concat was introduced", alias.child(), instanceOf(Concat.class));
        Concat concat = (Concat) alias.child();
        assertThat(concat.children().get(0), instanceOf(Literal.class));
        assertEquals(VIEW_NAME + "/", ((BytesRef) ((Literal) concat.children().get(0)).value()).utf8ToString());
        assertThat(concat.children().get(1), instanceOf(UnresolvedAttribute.class));
        assertEquals(IdFieldMapper.NAME, ((UnresolvedAttribute) concat.children().get(1)).name());
    }

    public void testDefaultMetadataFields_produceTypedNullAlias() {
        List<String> nullableFieldNames = MetadataAttribute.ATTRIBUTES_MAP.keySet()
            .stream()
            .filter(name -> MetadataAttribute.INDEX.equals(name) == false)
            .filter(name -> IdFieldMapper.NAME.equals(name) == false)
            .sorted()
            .toList();

        assertFalse("ATTRIBUTES_MAP must contain at least one nullable field", nullableFieldNames.isEmpty());

        for (String fieldName : nullableFieldNames) {
            MetadataAttribute field = (MetadataAttribute) MetadataAttribute.create(Source.EMPTY, fieldName);
            LogicalPlan result = ViewMetadataFieldRewriter.rewrite(VIEW_NAME, BODY, List.of(field));

            assertThat(fieldName + ": expected Eval node", result, instanceOf(Eval.class));
            Eval eval = (Eval) result;
            assertThat(fieldName + ": expected exactly one alias", eval.fields(), hasSize(1));
            Alias alias = eval.fields().getFirst();
            assertEquals(fieldName + ": alias name mismatch", fieldName, alias.name());
            assertThat(fieldName + ": expected null Literal", alias.child(), instanceOf(Literal.class));
            Literal lit = (Literal) alias.child();
            assertNull(fieldName + ": literal value must be null", lit.value());
            assertEquals(fieldName + ": literal type must match the field type", field.dataType(), lit.dataType());
        }
    }
}
