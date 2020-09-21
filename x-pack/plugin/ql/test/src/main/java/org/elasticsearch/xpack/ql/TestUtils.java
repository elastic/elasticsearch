/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.predicate.Range;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;
import static org.junit.Assert.assertEquals;

public final class TestUtils {

    public static final ZoneId UTC = ZoneId.of("Z");

    public static final Configuration TEST_CFG = new Configuration(UTC, null, null);

    private TestUtils() {}

    public static Configuration randomConfiguration() {
        return new Configuration(randomZone(), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static Configuration randomConfiguration(ZoneId zoneId) {
        return new Configuration(zoneId, randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static Literal of(Object value) {
        return of(Source.EMPTY, value);
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, DataTypes.fromJava(value));
    }

    public static Equals equalsOf(Expression left, Expression right) {
        return new Equals(EMPTY, left, right, randomZone());
    }

    public static NotEquals notEqualsOf(Expression left, Expression right) {
        return new NotEquals(EMPTY, left, right, randomZone());
    }

    public static NullEquals nullEqualsOf(Expression left, Expression right) {
        return new NullEquals(EMPTY, left, right, randomZone());
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, randomZone());
    }

    public static LessThanOrEqual lessThanOrEqualOf(Expression left, Expression right) {
        return new LessThanOrEqual(EMPTY, left, right, randomZone());
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, randomZone());
    }

    public static Range rangeOf(Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        return new Range(EMPTY, value, lower, includeLower, upper, includeUpper, randomZone());
    }

    //
    // Common methods / assertions
    //

    public static void assertNoSearchContexts(RestClient client) throws IOException {
        Map<String, Object> stats = searchStats(client);
        @SuppressWarnings("unchecked")
        Map<String, Object> indicesStats = (Map<String, Object>) stats.get("indices");
        for (String index : indicesStats.keySet()) {
            if (index.startsWith(".") == false) { // We are not interested in internal indices
                assertEquals(index + " should have no search contexts", 0, getOpenContexts(stats, index));
            }
        }
    }

    public static int getNumberOfSearchContexts(RestClient client, String index) throws IOException {
        return getOpenContexts(searchStats(client), index);
    }

    private static Map<String, Object> searchStats(RestClient client) throws IOException {
        Response response = client.performRequest(new Request("GET", "/_stats/search"));
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    @SuppressWarnings("unchecked")
    private static int getOpenContexts(Map<String, Object> stats, String index) {
        stats = (Map<String, Object>) stats.get("indices");
        stats = (Map<String, Object>) stats.get(index);
        stats = (Map<String, Object>) stats.get("total");
        stats = (Map<String, Object>) stats.get("search");
        return (Integer) stats.get("open_contexts");
    }
}
