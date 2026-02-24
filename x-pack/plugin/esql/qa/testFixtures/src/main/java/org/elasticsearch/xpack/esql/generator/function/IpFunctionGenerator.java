/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.function;

import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xpack.esql.generator.EsqlQueryGenerator.randomName;
import static org.elasticsearch.xpack.esql.generator.function.FunctionGeneratorUtils.fieldOrUnmapped;

/**
 * Generates random IP-related function expressions.
 */
public final class IpFunctionGenerator {

    private IpFunctionGenerator() {}

    /**
     * Generates an cidr_match function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String cidrMatchFunction(List<Column> columns, boolean allowUnmapped) {
        String ipField = fieldOrUnmapped(randomName(columns, Set.of("ip")), allowUnmapped);
        if (ipField == null) {
            return null;
        }
        String cidr = randomFrom("10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12", "0.0.0.0/0");
        return "cidr_match(" + ipField + ", \"" + cidr + "\")";
    }

    /**
     * Generates an ip_prefix function.
     * May randomly use unmapped field names to test NULL data type handling.
     *
     * @param columns the available columns
     * @param allowUnmapped if true, may use unmapped field names
     */
    public static String ipPrefixFunction(List<Column> columns, boolean allowUnmapped) {
        String ipField = fieldOrUnmapped(randomName(columns, Set.of("ip")), allowUnmapped);
        if (ipField == null) {
            return null;
        }
        return "ip_prefix(" + ipField + ", " + randomIntBetween(8, 32) + ", " + randomIntBetween(48, 128) + ")";
    }
}

