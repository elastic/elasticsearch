/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.arbitrary;

import org.elasticsearch.logsdb.datageneration.FieldType;

/**
 * Provides arbitrary values for different purposes.
 */
public interface Arbitrary {
    boolean generateSubObject();

    boolean generateNestedObject();

    int childFieldCount(int lowerBound, int upperBound);

    String fieldName(int lengthLowerBound, int lengthUpperBound);

    FieldType fieldType();

    long longValue();

    String stringValue(int lengthLowerBound, int lengthUpperBound);
}
