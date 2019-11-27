/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;

/**
 * Test validating the AST produced by the parser.
 */
public class AstTests extends ESTestCase {

    public void testBasicQuery() throws Exception {
        System.out.println(plan("process WHERE true"));
    }

    public void testBasicQueryWithSimpleFilter() throws Exception {
        System.out.println(plan("process WHERE process_name = 'svchost.exe' "));
    }

    public void testBasicQueryWithAndFilter() throws Exception {
        System.out.println(plan("process WHERE process_name == \"svchost.exe\" AND command_line != \"* -k *\""));
    }

    public void testBasicQueryWithIn() throws Exception {
        System.out.println(plan("process WHERE process_name IN ('ipconfig.exe', 'netstat.exe', 'systeminfo.exe', 'route.exe')"));
    }

    private static LogicalPlan plan(String query) {
        return new EqlParser().createStatement(query);
    }
}