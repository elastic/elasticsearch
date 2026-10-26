/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;

public class EsqlSearchExecutionContextTests extends MapperServiceTestCase {

    private static final Set<String> READ_BY_THE_PLAN = Set.of("mapped", "attrs.bar", "unmapped");
    private static final String MAPPING = """
        { "_doc": { "dynamic": false, "properties": {
          "mapped": { "type": "keyword" },
          "attrs": { "type": "flattened" }
        } } }""";

    /**
     * The shard resolves each field the plan reads that some shards may not map to what it holds: the field it maps, the key of a
     * flattened field, or a {@code keyword} read from {@code _source}.
     */
    public void testResolvesTheFieldsThePlanReads() throws IOException {
        EsqlSearchExecutionContext context = context();

        assertThat(context.getFieldType("mapped"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertFalse(context.readsFromSource("mapped"));

        assertThat(context.getFieldType("attrs.bar"), instanceOf(FlattenedFieldMapper.KeyedFlattenedFieldType.class));
        assertTrue("the shard reads the field, so a query on it is not rewritten to match nothing", context.isMappedField("attrs.bar"));
        assertFalse(context.readsFromSource("attrs.bar"));

        assertThat(context.getFieldType("unmapped"), instanceOf(KeywordScriptFieldType.class));
        assertTrue("the shard reads the field, so a query on it is not rewritten to match nothing", context.isMappedField("unmapped"));
        assertTrue(context.readsFromSource("unmapped"));
        assertThat("an exists query finds the field", context.getMatchingFieldNames("unmapped"), contains("unmapped"));
    }

    /** A name the plan does not read keeps the resolution of the mapping: the plan reads it as null. */
    public void testFieldsThePlanDoesNotReadAreNotResolved() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, Set.of());

        assertFalse(context.isMappedField("attrs.bar"));
        assertFalse(context.isMappedField("unmapped"));
        assertThat(context.getMatchingFieldNames("unmapped"), empty());
    }

    /** Field-level security hides an unmapped field as it hides a mapped one: the shard does not resolve it. */
    public void testFieldLevelSecurityHidesAnUnmappedField() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        base.setFieldVisibilityPredicate(name -> name.equals("unmapped") == false);
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, READ_BY_THE_PLAN);

        assertFalse(context.isMappedField("unmapped"));
        assertFalse(context.readsFromSource("unmapped"));
        assertThat(context.getMatchingFieldNames("unmapped"), empty());
    }

    private EsqlSearchExecutionContext context() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        return new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, READ_BY_THE_PLAN);
    }

}
