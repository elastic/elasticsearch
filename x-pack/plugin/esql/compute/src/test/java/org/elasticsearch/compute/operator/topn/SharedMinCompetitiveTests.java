/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class SharedMinCompetitiveTests extends ComputeTestCase {
    public void testEmpty() {
        SharedMinCompetitive minCompetitive = new SharedMinCompetitive(
            blockFactory().breaker(),
            List.of(ElementType.LONG),
            List.of(TopNEncoder.DEFAULT_SORTABLE),
            List.of(new TopNOperator.SortOrder(
                0, false, false
            ))
        );
    }
}
