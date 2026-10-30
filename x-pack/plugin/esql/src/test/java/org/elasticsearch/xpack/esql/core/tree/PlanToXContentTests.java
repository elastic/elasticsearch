/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PlanToXContentTests extends ESTestCase {

    public void testFilterOverRelation() throws IOException {
        Source source = new Source(3, 12, "pm25 > 0");
        EsRelation relation = relation(new Source(new Location(1, 0), "FROM idx", "logs_view"), "idx");
        ReferenceAttribute pm25 = new ReferenceAttribute(source, "pm25", DataType.INTEGER);
        GreaterThan condition = new GreaterThan(source, pm25, new Literal(source, 0, DataType.INTEGER), ZoneOffset.UTC);
        Filter filter = new Filter(source, relation, condition);

        String before = filter.toString();
        Map<String, Object> json = parse(filter.toJsonString());
        assertThat(filter.toString(), equalTo(before));

        assertThat(json.get("id"), equalTo(filter.planId().toString()));
        assertThat(json.get("type"), equalTo("Filter"));
        assertThat(json.get("role"), equalTo("logical"));
        assertThat(json.get("origin"), equalTo(List.of()));

        Map<String, Object> filterSource = map(json.get("source"));
        assertThat(filterSource.get("line"), equalTo(3));
        assertThat(filterSource.get("column"), equalTo(13));
        assertThat(filterSource.get("text"), equalTo("pm25 > 0"));
        assertThat(filterSource.get("view"), nullValue());

        Map<String, Object> properties = map(json.get("properties"));
        Map<String, Object> conditionJson = map(properties.get("condition"));
        assertThat(conditionJson.get("type"), equalTo("GreaterThan"));
        StringBuilder repr = new StringBuilder();
        condition.nodeString(repr, Node.NodeStringFormat.FULL, NodeStringMapper.IDENTITY);
        assertThat(conditionJson.get("repr"), equalTo(repr.toString()));
        assertThat(list(conditionJson.get("args")).size(), equalTo(2));
        Map<String, Object> conditionSource = map(conditionJson.get("source"));
        assertThat(conditionSource.get("text"), equalTo("pm25 > 0"));

        List<?> children = list(json.get("children"));
        assertThat(children.size(), equalTo(1));
        Map<String, Object> child = map(children.get(0));
        assertThat(child.get("type"), equalTo("EsRelation"));
        assertThat(child.get("id"), equalTo(relation.planId().toString()));
        assertThat(map(child.get("source")).get("view"), equalTo("logs_view"));
        assertThat(list(child.get("children")).isEmpty(), equalTo(true));

        Filter structuralCopy = new Filter(filter.source(), filter.child(), filter.condition());
        assertThat(structuralCopy.toString(), equalTo(filter.toString()));
        assertThat(structuralCopy.planId(), not(equalTo(filter.planId())));
    }

    public void testBinaryChildrenAreNotFlattened() throws IOException {
        Source source = new Source(1, 0, "left, right");
        EsRelation left = relation(source, "left");
        EsRelation right = relation(source, "right");
        Attribute key = new ReferenceAttribute(source, "k", DataType.KEYWORD);
        Join join = new Join(source, left, right, new JoinConfig(JoinTypes.LEFT, List.of(key), List.of(key), null), ExecuteLocation.ANY);

        Map<String, Object> json = parse(join.toJsonString());
        List<?> children = list(json.get("children"));
        assertThat(children.size(), equalTo(2));
        assertThat(map(children.get(0)).get("id"), equalTo(left.planId().toString()));
        assertThat(map(children.get(1)).get("id"), equalTo(right.planId().toString()));
        assertThat(map(map(children.get(0)).get("properties")).get("indexPattern"), equalTo("left"));
        assertThat(map(map(children.get(1)).get("properties")).get("indexPattern"), equalTo("right"));
    }

    public void testFragmentExecNestsLogicalPlan() throws IOException {
        Source source = new Source(2, 4, "a > 1");
        EsRelation relation = relation(source, "idx");
        Filter filter = new Filter(source, relation, new Literal(source, true, DataType.BOOLEAN));
        FragmentExec fragment = new FragmentExec(filter);

        Map<String, Object> json = parse(fragment.toJsonString());
        assertThat(json.get("type"), equalTo("FragmentExec"));
        assertThat(json.get("role"), equalTo("physical"));
        List<?> children = list(json.get("children"));
        assertThat(children.size(), equalTo(1));
        Map<String, Object> inner = map(children.get(0));
        assertThat(inner.get("type"), equalTo("Filter"));
        assertThat(inner.get("id"), equalTo(filter.planId().toString()));
        assertThat(inner.get("role"), equalTo("logical"));
        assertThat(list(inner.get("children")).size(), equalTo(1));
        assertThat(json.get("properties").toString(), not(org.hamcrest.Matchers.containsString("<>")));
    }

    public void testReplaceChildrenKeepsPlanId() {
        Source source = new Source(1, 0, "FROM idx | WHERE true");
        EsRelation relation = relation(source, "idx");
        Filter filter = new Filter(source, relation, new Literal(source, true, DataType.BOOLEAN));

        EsRelation other = relation(source, "other");
        Filter replaced = (Filter) filter.replaceChildren(List.of(other));
        assertThat(replaced.planId(), equalTo(filter.planId()));
        assertThat(replaced.planOrigin(), equalTo(filter.planOrigin()));
        assertThat(((EsRelation) replaced.child()).planId(), equalTo(other.planId()));

        Filter fresh = new Filter(filter.source(), filter.child(), filter.condition());
        assertThat(fresh.planId(), not(equalTo(filter.planId())));
        assertThat(fresh, equalTo(filter));
    }

    public void testTransformDownAndExpressionsKeepPlanId() {
        Source source = new Source(4, 1, "pm25 > 0");
        EsRelation relation = relation(source, "idx");
        Literal zero = new Literal(source, 0, DataType.INTEGER);
        Filter filter = new Filter(
            source,
            relation,
            new GreaterThan(source, new ReferenceAttribute(source, "pm25", DataType.INTEGER), zero, ZoneOffset.UTC)
        );

        Filter transformed = (Filter) filter.transformDown(
            EsRelation.class,
            r -> new EsRelation(
                r.source(),
                "other",
                r.indexMode(),
                r.originalIndices(),
                r.concreteIndices(),
                r.indexProperties(),
                r.output()
            )
        );
        assertThat(transformed.planId(), equalTo(filter.planId()));
        // Same-class rewrite of the relation keeps that node's id; only the index pattern changes.
        assertThat(((EsRelation) transformed.child()).planId(), equalTo(relation.planId()));
        assertThat(((EsRelation) transformed.child()).indexPattern(), equalTo("other"));

        Filter rewritten = (Filter) filter.transformExpressionsOnly(Literal.class, lit -> new Literal(lit.source(), 5, lit.dataType()));
        assertThat(rewritten.planId(), equalTo(filter.planId()));
        assertThat(rewritten, not(equalTo(filter)));
    }

    public void testNewTypeGetsNewId() {
        Source source = new Source(1, 0, "LIMIT 10");
        Filter filter = new Filter(source, relation(source, "idx"), new Literal(source, true, DataType.BOOLEAN));
        Limit limit = new Limit(source, new Literal(source, 10, DataType.INTEGER), filter);
        assertThat(limit.planId(), not(equalTo(filter.planId())));
        assertThat(limit.child(), sameInstance(filter));
        assertThat(limit.planOrigin(), equalTo(List.of()));
    }

    public void testProfileJsonEmbedsPlanObjectAndOperatorId() throws IOException {
        PlanProfile profile = new PlanProfile(
            "data",
            "elasticsearch",
            "node-1",
            "LimitExec\\_EsQueryExec",
            "Limit\\_EsRelation",
            null,
            "{\"id\":\"17\",\"type\":\"LimitExec\",\"children\":[]}",
            "{\"id\":\"9\",\"type\":\"Limit\",\"children\":[]}"
        );
        Map<String, Object> json = parse(Strings.toString(profile));
        assertThat(json.get("plan"), equalTo("LimitExec\\_EsQueryExec"));
        assertThat(json.get("plan_json"), instanceOf(Map.class));
        assertThat(map(json.get("plan_json")).get("type"), equalTo("LimitExec"));
        assertThat(json.get("logical_plan_json"), instanceOf(Map.class));

        OperatorStatus tagged = new OperatorStatus("LimitOperator[limit = 0/10]", null, "17");
        Map<String, Object> operator = parse(Strings.toString(tagged));
        assertThat(operator.get("operator"), equalTo("LimitOperator[limit = 0/10]"));
        assertThat(operator.get("plan_node_id"), equalTo("17"));

        OperatorStatus untagged = new OperatorStatus("OutputOperator", null);
        assertThat(parse(Strings.toString(untagged)).containsKey("plan_node_id"), equalTo(false));
    }

    private static EsRelation relation(Source source, String index) {
        return new EsRelation(source, index, IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
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

    private static List<?> list(Object value) {
        return (List<?>) value;
    }
}
