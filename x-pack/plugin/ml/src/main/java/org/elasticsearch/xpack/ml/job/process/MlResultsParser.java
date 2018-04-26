/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ToXContent;

import java.io.InputStream;
import java.util.Iterator;

public interface MlResultsParser<Result extends ToXContent> {

    Iterator<Result> parseResults(InputStream in) throws ElasticsearchParseException;
}
