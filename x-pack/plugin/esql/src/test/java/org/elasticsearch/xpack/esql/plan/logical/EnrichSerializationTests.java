/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FieldAttributeTests;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EnrichSerializationTests extends AbstractLogicalPlanSerializationTests<Enrich> {
    @Override
    protected Enrich createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        Enrich.Mode mode = randomFrom(Enrich.Mode.values());
        Expression policyName = randomPolicyName();
        NamedExpression matchField = FieldAttributeTests.createFieldAttribute(1, false);
        EnrichPolicy policy = randomEnrichPolicy();
        Map<String, String> concreteIndices = randomConcreteIndices();
        List<NamedExpression> enrichFields = randomFieldAttributes(0, 10, false).stream().map(f -> (NamedExpression) f).toList();
        return new Enrich(source, child, mode, policyName, matchField, policy, concreteIndices, enrichFields);
    }

    private static Expression randomPolicyName() {
        return new Literal(randomSource(), randomAlphaOfLength(5), DataType.KEYWORD);
    }

    private static EnrichPolicy randomEnrichPolicy() {
        String type = randomFrom(EnrichPolicy.SUPPORTED_POLICY_TYPES);
        EnrichPolicy.QuerySource query = randomBoolean() ? randomQuerySource() : null;
        List<String> indices = randomList(0, 5, () -> randomAlphaOfLength(5));
        String matchField = randomAlphaOfLength(5);
        List<String> enrichFields = randomList(0, 5, () -> randomAlphaOfLength(5));
        return new EnrichPolicy(type, query, indices, matchField, enrichFields);
    }

    private static EnrichPolicy.QuerySource randomQuerySource() {
        QueryBuilder queryBuilder = randomBoolean()
            ? new MatchAllQueryBuilder()
            : new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(4));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON, out)) {
            XContentBuilder content = queryBuilder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            content.flush();
            return new EnrichPolicy.QuerySource(new BytesArray(out.toByteArray()), content.contentType());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> randomConcreteIndices() {
        int size = between(0, 5);
        Map<String, String> result = new HashMap<>();
        while (result.size() < size) {
            result.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return result;
    }

    @Override
    protected Enrich mutateInstance(Enrich instance) throws IOException {
        Source source = instance.source();
        LogicalPlan child = instance.child();
        Enrich.Mode mode = instance.mode();
        Expression policyName = instance.policyName();
        NamedExpression matchField = instance.matchField();
        EnrichPolicy policy = instance.policy();
        Map<String, String> concreteIndices = instance.concreteIndices();
        List<NamedExpression> enrichFields = instance.enrichFields();
        switch (between(0, 6)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> mode = randomValueOtherThan(mode, () -> randomFrom(Enrich.Mode.values()));
            case 2 -> policyName = randomValueOtherThan(policyName, EnrichSerializationTests::randomPolicyName);
            case 3 -> matchField = randomValueOtherThan(matchField, () -> FieldAttributeTests.createFieldAttribute(1, false));
            case 4 -> policy = randomValueOtherThan(policy, EnrichSerializationTests::randomEnrichPolicy);
            case 5 -> concreteIndices = randomValueOtherThan(concreteIndices, EnrichSerializationTests::randomConcreteIndices);
            case 6 -> enrichFields = randomValueOtherThan(
                enrichFields,
                () -> randomFieldAttributes(0, 10, false).stream().map(f -> (NamedExpression) f).toList()
            );
            default -> throw new IllegalArgumentException();
        }
        return new Enrich(source, child, mode, policyName, matchField, policy, concreteIndices, enrichFields);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
