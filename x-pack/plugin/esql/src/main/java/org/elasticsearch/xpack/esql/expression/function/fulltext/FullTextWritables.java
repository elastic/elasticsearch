/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FullTextWritables {

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();

        entries.add(QueryString.ENTRY);
        entries.add(Match.ENTRY);
        entries.add(MultiMatch.ENTRY);
        entries.add(Kql.ENTRY);

        if (EsqlCapabilities.Cap.TERM_FUNCTION.isEnabled()) {
            entries.add(Term.ENTRY);
        }
        if (EsqlCapabilities.Cap.SCORE_FUNCTION.isEnabled()) {
            entries.add(Score.ENTRY);
        }

        return Collections.unmodifiableList(entries);
    }
}
