/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyPolicyResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;

//@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class QueryTranslatorTests extends ESTestCase {

    private static TestPlannerOptimizer plannerOptimizer;

    private static TestPlannerOptimizer plannerOptimizerIPs;

    private static Analyzer makeAnalyzer(String mappingFileName) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);

        return new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                emptyPolicyResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L))
        );
    }

    @BeforeClass
    public static void init() {
        plannerOptimizer = new TestPlannerOptimizer(EsqlTestUtils.TEST_CFG, makeAnalyzer("mapping-all-types.json"));
        plannerOptimizerIPs = new TestPlannerOptimizer(EsqlTestUtils.TEST_CFG, makeAnalyzer("mapping-hosts.json"));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void assertQueryTranslation(String query, Matcher<String> translationMatcher) {
        PhysicalPlan optimized = plannerOptimizer.plan(query);
        EsQueryExec eqe = (EsQueryExec) optimized.collectLeaves().get(0);
        final String translatedQuery = eqe.query().toString().replaceAll("\\s+", "");
        assertThat(translatedQuery, translationMatcher);
    }

    public void assertQueryTranslationIPs(String query, Matcher<String> translationMatcher) {
        PhysicalPlan optimized = plannerOptimizerIPs.plan(query);
        EsQueryExec eqe = (EsQueryExec) optimized.collectLeaves().get(0);
        final String translatedQuery = eqe.query().toString().replaceAll("\\s+", "");
        assertThat(translatedQuery, translationMatcher);
    }

    public void testBinaryComparisons() {
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer""", containsString("""
            "esql_single_value":{"field":"integer","next":{"range":{"integer":{"gt":10,"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.0 <= double""", containsString("""
            esql_single_value":{"field":"double","next":{"range":{"double":{"gte":10.0,"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" > date""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"lt":"2007-12-03T09:15:30.000Z","time_zone":"Z","""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long > unsigned_long""", containsString("""
            "esql_single_value":{"field":"unsigned_long","next":{"range":{"unsigned_long":{"lt":2147483648,"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648 >= long""", containsString("""
            "esql_single_value":{"field":"long","next":{"range":{"long":{"lte":2147483648,"""));

        assertQueryTranslation("""
            FROM test | WHERE "1.2.3" == version""", containsString("""
            "esql_single_value":{"field":"version","next":{"term":{"version":{"value":"1.2.3","boost":0.0}"""));

        assertQueryTranslation("""
            FROM test | WHERE "foo" == keyword""", containsString("""
            "esql_single_value":{"field":"keyword","next":{"term":{"keyword":{"value":"foo","boost":0.0}"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" == date""", containsString("""
            "esql_single_value":{"field":"date","next":{"term":{"date":{"value":"2007-12-03T09:15:30.000Z"""));

        assertQueryTranslation("""
            FROM test | WHERE ip != "127.0.0.1\"""", containsString("""
            "esql_single_value":{"field":"ip","next":{"bool":{"must_not":[{"term":{"ip":{"value":"127.0.0.1","boost":0.0}}"""));
    }

    public void testRanges() {
        // ORs
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer OR integer < 12""", matchesRegex("""
            .*should.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"gt":10,.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 <= integer OR integer < 12""", matchesRegex("""
            .*should.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"gte":10,.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 < integer OR integer <= 12""", matchesRegex("""
            .*should.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"gt":10,.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"lte":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 <= integer OR integer <= 12""", matchesRegex("""
            .*should.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"gte":10,.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"lte":12.*"""));

        // ANDs
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer AND integer < 12""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"integer","next":\\{"range":\\{"integer":\\{"gt":10,"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 <= integer AND integer <= 12""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"integer","next":\\{"range":\\{"integer":\\{"gte":10,"lte":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 <= integer AND integer < 12""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"integer","next":\\{"range":\\{"integer":\\{"gte":10,"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.9 < double AND double < 12.1""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"double","next":\\{"range":\\{"double":\\{"gt":10.9,"lt":12.1.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.9 <= double AND double <= 12.1""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"double","next":\\{"range":\\{"double":\\{"gte":10.9,"lte":12.1.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.9 < double AND double <= 12.1""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"double","next":\\{"range":\\{"double":\\{"gt":10.9,"lte":12.1.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long < unsigned_long AND unsigned_long < 2147483650::unsigned_long""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"unsigned_long".*\\{"range":\\{"unsigned_long":\\{"gt":2147483648,"lt":2147483650.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long <= unsigned_long AND unsigned_long <= 2147483650::unsigned_long""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"unsigned_long".*\\{"range":\\{"unsigned_long":\\{"gte":2147483648,"lte":2147483650.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long <= unsigned_long AND unsigned_long <= 2147483650::unsigned_long""", matchesRegex("""
            \\{"esql_single_value":\\{"field":"unsigned_long".*\\{"range":\\{"unsigned_long":\\{"gte":2147483648,"lte":2147483650.*"""));

        // mixed ANDs and NotEquals
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer AND integer < 12 AND integer > 0 AND integer != 5""", matchesRegex("""
            .*bool.*must.*""" + """
            esql_single_value":\\{"field":"integer","next":\\{"bool":""" + """
            .*must_not.*\\[\\{"term":\\{"integer":\\{"value":5.*""" + """
            esql_single_value":\\{"field":"integer","next":\\{"range":\\{"integer":\\{"gt":10,"lt":12.*"""));

        // multiple Ranges
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer AND double < 1.0 AND integer < 12 AND double > -1.0""", matchesRegex("""
            .*bool.*must.*""" + """
            esql_single_value":\\{"field":"integer","next":\\{"range":\\{"integer":\\{"gt":10,"lt":12.*""" + """
            esql_single_value":\\{"field":"double","next":\\{"range":\\{"double":\\{"gt":-1.0,"lt":1.0.*"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30Z" <= date AND date <= "2024-01-01T10:15:30\"""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"gte":"2007-12-03T10:15:30.000Z","lte":"2024-01-01T10:15:30.000Z",\
            "time_zone":"Z","format":"strict_date_optional_time","boost":0.0}}}"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30" <= date AND date <= "2024-01-01T10:15:30Z\"""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"gte":"2007-12-03T10:15:30.000Z","lte":"2024-01-01T10:15:30.000Z",\
            "time_zone":"Z","format":"strict_date_optional_time","boost":0.0}}}"""));

        // various timezones
        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" < date AND date < "2024-01-01T10:15:30+01:00\"""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"gt":"2007-12-03T09:15:30.000Z","lt":"2024-01-01T09:15:30.000Z",\
            "time_zone":"Z","format":"strict_date_optional_time","boost":0.0}}}"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30-01:00" <= date AND date <= "2024-01-01T10:15:30+01:00\"""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"gte":"2007-12-03T11:15:30.000Z","lte":"2024-01-01T09:15:30.000Z",\
            "time_zone":"Z","format":"strict_date_optional_time","boost":0.0}}}"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30" <= date AND date <= "2024-01-01T10:15:30+01:00\"""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"gte":"2007-12-03T10:15:30.000Z","lte":"2024-01-01T09:15:30.000Z",\
            "time_zone":"Z","format":"strict_date_optional_time","boost":0.0}}}"""));
    }

    public void testIPs() {
        // Nothing to combine
        assertQueryTranslationIPs("""
            FROM hosts | WHERE CIDR_MATCH(ip0, "127.0.0.1") OR CIDR_MATCH(ip0, "127.0.0.3") AND CIDR_MATCH(ip1, "fe80::cae2:65ff:fece:fec0")
            """, matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1".*""" + """
            .*bool.*must.*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.3".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["fe80::cae2:65ff:fece:fec0".*"""));

        // ANDs
        assertQueryTranslationIPs("""
            FROM hosts | WHERE ip1 >= "127.0.0.1" AND ip1 <= "128.0.0.1" \
            AND ip0 > "127.0.0.1" AND  ip0 < "128.0.0.1\"""", matchesRegex("""
            .*bool.*must.*""" + """
            esql_single_value":\\{"field":"ip1".*"range":\\{"ip1":\\{"gte":"127.0.0.1","lte":"128.0.0.1".*""" + """
            esql_single_value":\\{"field":"ip0".*"range":\\{"ip0":\\{"gt":"127.0.0.1","lt":"128.0.0.1".*"""));

        // ORs - Combine Equals, In and CIDRMatch on IP type
        assertQueryTranslationIPs("""
            FROM hosts | WHERE host == "alpha" OR host == "gamma" OR CIDR_MATCH(ip1, "127.0.0.2/32") OR CIDR_MATCH(ip1, "127.0.0.3/32") \
            OR card IN ("eth0", "eth1") OR card == "lo0" OR CIDR_MATCH(ip0, "127.0.0.1") OR \
            CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:feb9") OR host == "beta\"""", matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"host".*"terms":\\{"host":\\["alpha","gamma","beta".*""" + """
            esql_single_value":\\{"field":"card".*"terms":\\{"card":\\["eth0","eth1","lo0".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["127.0.0.2/32","127.0.0.3/32".*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1","fe80::cae2:65ff:fece:feb9".*"""));

        assertQueryTranslationIPs("""
            FROM hosts | WHERE host == "alpha" OR host == "gamma" OR CIDR_MATCH(ip1, "127.0.0.2/32") OR CIDR_MATCH(ip1, "127.0.0.3/32") \
            OR card IN ("eth0", "eth1") OR card == "lo0" OR CIDR_MATCH(ip0, "127.0.0.1") OR \
            CIDR_MATCH(ip0, "127.0.0.0/24", "172.0.0.0/31") OR CIDR_MATCH(ip0, "127.0.1.0/24") OR \
            CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:fec0", "172.0.2.0/24") OR \
            CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:feb9") OR host == "beta\"""", matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"host".*"terms":\\{"host":\\["alpha","gamma","beta".*""" + """
            esql_single_value":\\{"field":"card".*"terms":\\{"card":\\["eth0","eth1","lo0".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["127.0.0.2/32","127.0.0.3/32".*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1","127.0.0.0/24","172.0.0.0/31","127.0.1.0/24",\
            "fe80::cae2:65ff:fece:fec0","172.0.2.0/24","fe80::cae2:65ff:fece:feb9".*"""));

        assertQueryTranslationIPs("""
            FROM hosts | WHERE host == "alpha" OR host == "gamma" OR ip1 IN ("127.0.0.2"::ip) OR CIDR_MATCH(ip1, "127.0.0.3/32") \
            OR card IN ("eth0", "eth1") OR card == "lo0" OR ip0 IN ("127.0.0.1"::ip, "128.0.0.1"::ip) \
            OR CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:feb9") OR host == "beta\"""", matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"host".*"terms":\\{"host":\\["alpha","gamma","beta".*""" + """
            esql_single_value":\\{"field":"card".*"terms":\\{"card":\\["eth0","eth1","lo0".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["127.0.0.3/32","127.0.0.2".*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1","128.0.0.1","fe80::cae2:65ff:fece:feb9".*"""));

        assertQueryTranslationIPs("""
            FROM hosts | WHERE host == "alpha" OR host == "gamma" OR ip1 == "127.0.0.2"::ip OR CIDR_MATCH(ip1, "127.0.0.3/32") \
            OR card IN ("eth0", "eth1") OR card == "lo0" OR ip0 IN ("127.0.0.1"::ip, "128.0.0.1"::ip) \
            OR CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:feb9") OR host == "beta\"""", matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"host".*"terms":\\{"host":\\["alpha","gamma","beta".*""" + """
            esql_single_value":\\{"field":"card".*"terms":\\{"card":\\["eth0","eth1","lo0".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["127.0.0.3/32","127.0.0.2".*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1","128.0.0.1","fe80::cae2:65ff:fece:feb9".*"""));

        assertQueryTranslationIPs("""
            FROM hosts | WHERE host == "alpha" OR host == "gamma" OR ip1 == "127.0.0.2" OR CIDR_MATCH(ip1, "127.0.0.3/32") \
            OR card IN ("eth0", "eth1") OR card == "lo0" OR ip0 IN ("127.0.0.1"::ip, "128.0.0.1"::ip) \
            OR CIDR_MATCH(ip0, "fe80::cae2:65ff:fece:feb9") OR host == "beta\"""", matchesRegex("""
            .*bool.*should.*""" + """
            esql_single_value":\\{"field":"host".*"terms":\\{"host":\\["alpha","gamma","beta".*""" + """
            esql_single_value":\\{"field":"card".*"terms":\\{"card":\\["eth0","eth1","lo0".*""" + """
            esql_single_value":\\{"field":"ip1".*"terms":\\{"ip1":\\["127.0.0.3/32","127.0.0.2".*""" + """
            esql_single_value":\\{"field":"ip0".*"terms":\\{"ip0":\\["127.0.0.1","128.0.0.1","fe80::cae2:65ff:fece:feb9".*"""));
    }
}
