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

import static org.hamcrest.Matchers.instanceOf;

public class EsqlSearchExecutionContextTests extends MapperServiceTestCase {

    private static final Set<String> READ_BY_THE_PLAN = Set.of("mapped", "attrs.bar", "unmapped", "object");
    private static final String MAPPING = """
        { "_doc": { "dynamic": false, "properties": {
          "mapped": { "type": "keyword" },
          "attrs": { "type": "flattened" },
          "object": { "properties": { "leaf": { "type": "keyword" } } }
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

        assertNull("an object holds no value of its own, so the shard reads none", context.getFieldType("object"));
        assertFalse(context.readsFromSource("object"));
    }

    /** A name the plan does not read keeps the resolution of the mapping: the plan reads it as null. */
    public void testFieldsThePlanDoesNotReadAreNotResolved() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, Set.of());

        assertFalse(context.isMappedField("attrs.bar"));
        assertFalse(context.isMappedField("unmapped"));
    }

    /** Where the query reads keys, a name under a flattened field is its key, whatever the plan reads. */
    public void testReadsAKeyWhereTheQueryReadsKeys() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, Set.of(), true);

        assertTrue(context.readsKey("attrs.bar"));
        assertTrue(context.isMappedField("attrs.bar"));
        assertFalse(context.readsKey("mapped"));
        assertFalse(context.readsKey("unmapped"));
        assertFalse(new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, READ_BY_THE_PLAN).readsKey("attrs.bar"));
    }

    /** Field-level security hides a key as it hides a mapped field: the shard does not read it. */
    public void testFieldLevelSecurityHidesAKey() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        base.setFieldVisibilityPredicate(name -> name.equals("attrs.bar") == false);
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, Set.of("attrs.bar"), true);

        assertFalse(context.readsKey("attrs.bar"));
        assertFalse(context.isMappedField("attrs.bar"));
        assertFalse(context.isFieldMapped("attrs.bar"));
        assertTrue(context.readsKey("attrs.baz"));
    }

    /** Field-level security hides an unmapped field as it hides a mapped one: the shard does not resolve it. */
    public void testFieldLevelSecurityHidesAnUnmappedField() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        base.setFieldVisibilityPredicate(name -> name.equals("unmapped") == false);
        EsqlSearchExecutionContext context = new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, READ_BY_THE_PLAN);

        assertFalse(context.isMappedField("unmapped"));
        assertFalse(context.readsFromSource("unmapped"));
    }

    private EsqlSearchExecutionContext context() throws IOException {
        SearchExecutionContext base = createSearchExecutionContext(createMapperService(MAPPING));
        return new EsqlSearchExecutionContext(base, QueryWarnings.EMIT, READ_BY_THE_PLAN);
    }

}
