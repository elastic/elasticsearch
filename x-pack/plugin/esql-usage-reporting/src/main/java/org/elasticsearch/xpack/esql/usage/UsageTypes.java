/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

/**
 * The usage type strings. The transform that interprets usage is keyed on the exact string, so a record with a different one is not
 * counted; the value is to be confirmed with the Usage API owners and lives only here.
 */
public final class UsageTypes {

    /** CPU time of queries over external sources. */
    public static final String EXTERNAL_SOURCES_CPU = "es_esql_external_sources_cpu";

    private UsageTypes() {}
}
