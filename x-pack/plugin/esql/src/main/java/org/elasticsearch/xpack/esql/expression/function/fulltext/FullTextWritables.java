/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;

import java.util.List;

public class FullTextWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(QueryString.ENTRY, Match.ENTRY);
    }

}
