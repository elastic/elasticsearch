/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.analysis.Analyzer;

import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyze;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;

public class OptimizerVerificationTests extends AbstractLogicalPlanOptimizerTests {
    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/155979 where ip
     * ended up as unresolved in the logical plan optimizations
     */
    public void testOrCidrMatchNotPruned() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));

        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip, TO_STRING(ip0), null)
            """, analyzer));
    }

    /**
     * Regression test for https://github.com/elastic/elasticsearch/issues/155979 where ip
     * disappeared from the plan in the logical optimizations
     */
    public void testOrCidrMatchNotPruned2() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));

        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip, TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """, analyzer));
    }

    public void testOrCidrMatchWithInNotPruned() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));

        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null)
            """, analyzer));
    }

    public void testOrCidrMatchWithInNotPruned2() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));

        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """, analyzer));
    }

    public void testOrCidrMatchWithMixedTypeInNotPruned() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));
        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 IN ("127.0.0.1"::ip, "192.168.1.1"), TO_STRING(ip0), null)
            """, analyzer));
    }

    public void testIpInWithoutCidrMatchNotPruned() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));
        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(ip0 IN ("127.0.0.1"::ip, "192.168.1.1"::ip), TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """, analyzer));
    }

    public void testIpEqualityAndInCombinedWithCidrMatchNotPruned() {
        Analyzer analyzer = analyzer(loadMapping("mapping-hosts.json", "hosts"));
        optimize(analyze("""
            FROM hosts
            | EVAL ip = CASE(
                CIDR_MATCH(ip0, "10.0.0.0/8") OR ip0 == "127.0.0.1"::ip OR ip0 IN ("192.168.1.1"::ip, "172.16.0.1"::ip),
                TO_STRING(ip0), null),
                   field = CASE(ip IS NOT NULL, "a", "b")
            | STATS count = COUNT(*) BY field
            """, analyzer));
    }

}
