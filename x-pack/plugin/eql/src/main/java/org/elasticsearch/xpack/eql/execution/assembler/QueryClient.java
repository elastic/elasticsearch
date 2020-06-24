/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.eql.execution.payload.Payload;

/**
 * Infrastructure interface used to decouple listener consumers from the stateful classes holding client-references and co.
 */
interface QueryClient {

    void query(SearchSourceBuilder searchSource, ActionListener<Payload<SearchHit>> listener);
}
