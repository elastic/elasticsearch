/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import java.io.IOException;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
 * Reads an object from a parser using some context.
 */
@FunctionalInterface
public interface ContextParser<Context, T> {
    T parse(XContentParser p, Context c) throws IOException;
}
