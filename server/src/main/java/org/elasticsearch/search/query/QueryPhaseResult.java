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

/**
 * Includes result returned by a search operation as part of the query phase.
 * @param topDocsAndMaxScore the {@link org.apache.lucene.search.TopDocs} as wel as the optional maximum score
 * @param sortValueFormats the fields that the request was sorted on
 * @param terminatedAfter whether the request was early terminated, based on the <code>terminate_after</code> functionality
 * @param collectorResult the profile result (when profiling was enabled)
 */
public record QueryPhaseResult(
    TopDocsAndMaxScore topDocsAndMaxScore,
    DocValueFormat[] sortValueFormats,
    boolean terminatedAfter,
    CollectorResult collectorResult
) {}
