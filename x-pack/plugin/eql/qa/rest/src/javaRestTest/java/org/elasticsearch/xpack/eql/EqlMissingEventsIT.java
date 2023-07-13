/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.eql.EqlMissingEventsSpecTestCase;

import java.util.List;

@LuceneTestCase.AwaitsFix(bugUrl = "xxx")
public class EqlMissingEventsIT extends EqlMissingEventsSpecTestCase {

    public EqlMissingEventsIT(String query, String name, List<long[]> eventIds, String[] joinKeys, Integer size, Integer maxSamplesPerKey) {
        super(query, name, eventIds, joinKeys, size, maxSamplesPerKey);
    }

}
