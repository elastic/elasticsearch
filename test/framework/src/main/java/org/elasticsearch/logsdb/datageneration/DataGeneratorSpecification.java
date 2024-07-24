/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;
import org.elasticsearch.logsdb.datageneration.arbitrary.RandomBasedArbitrary;

/**
 * Allows configuring behavior of {@link  DataGenerator}.
 * @param maxFieldCountPerLevel maximum number of fields that an individual object in mapping has.
 *                              Applies to subobjects.
 * @param maxObjectDepth maximum depth of nested objects
 * @param arbitrary provides arbitrary values used during generation
 */
public record DataGeneratorSpecification(int maxFieldCountPerLevel, int maxObjectDepth, Arbitrary arbitrary) {
    public DataGeneratorSpecification() {
        this(50, 3, new RandomBasedArbitrary());
    }
}
