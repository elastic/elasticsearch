/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class MapperOriginTests extends ESTestCase {

    public void testUnaryTranslationsRecordLogicalIds() throws IOException {
        Source source = new Source(1, 0, "FROM idx | WHERE a > 1 | EVAL b = 1 | KEEP b");
        EsRelation relation = new EsRelation(source, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        ReferenceAttribute a = new ReferenceAttribute(source, "a", DataType.INTEGER);
        Filter filter = new Filter(source, relation, new GreaterThan(source, a, new Literal(source, 1, DataType.INTEGER), ZoneOffset.UTC));
        Alias b = new Alias(source, "b", new Literal(source, 1, DataType.INTEGER));
        Eval eval = new Eval(source, filter, List.of(b));
        Project project = new Project(source, eval, List.of(b.toAttribute()));

        FragmentExec scan = new FragmentExec(relation);
        FilterExec filterExec = as(MapperUtils.mapUnary(filter, scan), FilterExec.class);
        EvalExec evalExec = as(MapperUtils.mapUnary(eval, filterExec), EvalExec.class);
        ProjectExec projectExec = as(MapperUtils.mapUnary(project, evalExec), ProjectExec.class);

        assertThat(filterExec.planOrigin(), hasItem(filter.planId()));
        assertThat(evalExec.planOrigin(), hasItem(eval.planId()));
        assertThat(projectExec.planOrigin(), hasItem(project.planId()));

        Map<String, Object> json = parse(projectExec.toJsonString());
        assertThat(json.get("type"), equalTo("ProjectExec"));
        assertThat(list(json.get("origin")), hasItem(project.planId().toString()));
        Map<String, Object> evalJson = map(list(json.get("children")).get(0));
        assertThat(evalJson.get("type"), equalTo("EvalExec"));
        assertThat(list(evalJson.get("origin")), hasItem(eval.planId().toString()));
        Map<String, Object> filterJson = map(list(evalJson.get("children")).get(0));
        assertThat(filterJson.get("type"), equalTo("FilterExec"));
        assertThat(list(filterJson.get("origin")), hasItem(filter.planId().toString()));
    }

    public void testRelationBecomesFragmentWithOrigin() {
        Source source = new Source(1, 0, "FROM idx");
        EsRelation relation = new EsRelation(source, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        PhysicalPlan mapped = new Mapper().map(new Versioned<>(relation, TransportVersion.current()));
        FragmentExec fragment = as(mapped, FragmentExec.class);
        assertThat(fragment.planOrigin(), hasItem(relation.planId()));
        assertThat(fragment.fragment().planId(), equalTo(relation.planId()));
    }

    public void testStreamingFilterIsFragmentOfThatFilter() {
        Source source = new Source(1, 0, "FROM idx | WHERE true");
        EsRelation relation = new EsRelation(source, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
        Filter filter = new Filter(source, relation, new Literal(source, true, DataType.BOOLEAN));
        PhysicalPlan mapped = new Mapper().map(new Versioned<>(filter, TransportVersion.current()));
        FragmentExec fragment = as(mapped, FragmentExec.class);
        assertThat(fragment.planOrigin(), hasItem(filter.planId()));
        assertThat(fragment.fragment(), org.hamcrest.Matchers.sameInstance(filter));
    }

    private Map<String, Object> parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return parser.map();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<String> list(Object value) {
        return (List<String>) value;
    }

}
