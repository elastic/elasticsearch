/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parses XContent into a {@link ScoreFunctionBuilder}.
 */
@FunctionalInterface
public interface ScoreFunctionParser<FB extends ScoreFunctionBuilder<FB>> {
    FB fromXContent(XContentParser parser) throws IOException;
}
