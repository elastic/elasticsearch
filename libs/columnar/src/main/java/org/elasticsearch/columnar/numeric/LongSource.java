/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import java.io.IOException;

/**
 * A source of {@code long} values addressed by position, not by document. Used for flat sequences
 * such as a dictionary column's ordinals, where the grouping by document is already recorded elsewhere
 * and only the values themselves need encoding.
 */
@FunctionalInterface
public interface LongSource {
    long next() throws IOException;
}
