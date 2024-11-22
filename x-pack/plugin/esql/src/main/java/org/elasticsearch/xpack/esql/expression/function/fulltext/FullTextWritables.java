/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MultiMatchQueryPredicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FullTextWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        entries.add(MatchQueryPredicate.ENTRY);
        entries.add(MultiMatchQueryPredicate.ENTRY);
        entries.add(QueryString.ENTRY);
        entries.add(Match.ENTRY);

        if (EsqlCapabilities.Cap.KQL_FUNCTION.isEnabled()) {
            entries.add(Kql.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
