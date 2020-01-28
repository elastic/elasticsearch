/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InferenceFetchSubPhase implements FetchSubPhase {

    private static Logger logger = LogManager.getLogger(InferenceFetchSubPhase.class);

    public InferenceFetchSubPhase() {
        logger.info("creating InferenceFetchSubPhase");
    }

    public void hitExecute(SearchContext context, HitContext hitContext) throws IOException {
        logger.info("hitcontex");

            Map<String, DocumentField> fields = new HashMap<>(hitContext.hit().getFields());
            fields.put("Hello", new DocumentField("teddy", List.of("ruxpin")));
            hitContext.hit().fields(fields);
    }

    public void hitsExecute(SearchContext context, SearchHit[] hits) throws IOException {
        // do something to a search hit

        logger.info("modifying hits");

        for (SearchHit hit : hits) {
            Map<String, DocumentField> fields = new HashMap<>(hit.getFields());
            fields.put("chocolate", new DocumentField("foo", List.of("bar")));
            hit.fields(fields);
        }
    }
}
