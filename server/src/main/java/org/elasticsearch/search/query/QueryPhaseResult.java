/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.profile.query.CollectorResult;

//TODO javadocs
public record QueryPhaseResult(
    TopDocsAndMaxScore topDocsAndMaxScore,
    DocValueFormat[] sortValueFormats,
    boolean terminatedAfter,
    CollectorResult collectorResult
) {}
