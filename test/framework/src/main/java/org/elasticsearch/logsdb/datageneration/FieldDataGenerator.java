/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Entity responsible for generating a valid randomized mapping for a field
 * and a generator of field values valid for this mapping.
 *
 * Generator is expected to produce the same mapping per instance of generator.
 * Function returned by {@link FieldDataGenerator#fieldValueGenerator() } is expected
 * to produce a randomized value each time.
 */
public interface FieldDataGenerator {
    CheckedConsumer<XContentBuilder, IOException> mappingWriter();

    CheckedConsumer<XContentBuilder, IOException> fieldValueGenerator();
}
