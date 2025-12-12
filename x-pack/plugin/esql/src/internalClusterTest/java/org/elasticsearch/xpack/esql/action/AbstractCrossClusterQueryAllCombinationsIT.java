/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractCrossClusterQueryAllCombinationsIT extends AbstractCrossClusterTestCase {
    Logger LOGGER = LogManager.getLogger(AbstractCrossClusterQueryAllCombinationsIT.class);

    // expected results for skip_unavailable = true and false respectively
    static final Map<String, String[]> expected = Map.ofEntries(
        e("logs-1", "OK", "OK"),
        e("logs-1*", "OK", "OK"),
        e("logs-1,logs-1*", "OK", "OK"),
        e("nonex", "Failure", "Failure"),
        e("logs-1,nonex", "Failure", "Failure"),
        e("logs-1*,nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex", "Failure", "Failure"),
        e("nonex*", "Failure", "Failure"),
        e("logs-1,nonex*", "OK", "OK"),
        e("logs-1*,nonex*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*", "OK", "OK"),
        e("nonex,nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2", "OK", "OK"),
        e("logs-1,cluster-a:logs-2", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2", "OK", "OK"),
        e("nonex,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2", "Failure", "Failure"),
        e("cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2*", "OK", "OK"),
        e("nonex,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2*", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("nonex,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*", "Failure", "Failure"),
        e("cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:nonex", "Failure", "Failure"),
        e("nonex*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:nonex", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("nonex,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex", "Failure", "Failure"),
        e("cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex", "Failure", "Failure"),
        e("cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("nonex,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "OK", "OK"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("logs-1,logs-1*,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "OK", "Failure"),
        e("nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure"),
        e("logs-1,logs-1*,nonex,nonex*,cluster-a:logs-2,cluster-a:logs-2*,cluster-a:nonex,cluster-a:nonex*", "Failure", "Failure")
    );

    protected abstract boolean skipUnavailable();

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, skipUnavailable(), REMOTE_CLUSTER_2, skipUnavailable());
    }

    public void test() throws Exception {
        setupTwoClusters();

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();

        List<String> patterns = List.of(
            LOCAL_INDEX,
            LOCAL_INDEX + "*",
            "nonex",
            "nonex*",
            REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX,
            REMOTE_CLUSTER_1 + ":" + REMOTE_INDEX + "*",
            REMOTE_CLUSTER_1 + ":nonex",
            REMOTE_CLUSTER_1 + ":nonex*"
        );

        for (int i = 1; i < 1 << patterns.size(); i++) {
            StringBuilder pattern = new StringBuilder();
            for (int j = 0; j < patterns.size(); j++) {
                if ((i & (1 << j)) != 0) {
                    if (pattern.length() > 0) {
                        pattern.append(",");
                    }
                    pattern.append(patterns.get(j));
                }
            }
            try (EsqlQueryResponse resp = runQuery("from " + pattern, requestIncludeMeta)) {
                assertThat(
                    "Unexpected result for pattern " + pattern,
                    expected.get(pattern.toString())[skipUnavailable() ? 0 : 1],
                    equalTo("OK")
                );
                // LOGGER.error(pattern + " \t OK");
            } catch (Exception e) {
                assertThat(
                    "Unexpected result for pattern " + pattern,
                    expected.get(pattern.toString())[skipUnavailable() ? 0 : 1],
                    equalTo("Failure")
                );
                // LOGGER.error(pattern + " \t Failure");
            }
        }
    }

    Map<String, Object> setupTwoClusters() throws IOException {
        return setupClusters(2);
    }

    private static Map.Entry<String, String[]> e(String pattern, String skipUnavailable, String noSkipUnavailable) {
        return Map.entry(pattern, new String[] { skipUnavailable, noSkipUnavailable });
    }

}
