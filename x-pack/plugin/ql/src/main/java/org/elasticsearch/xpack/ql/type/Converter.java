/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.common.io.stream.NamedWriteable;

import java.time.ZoneId;

public interface Converter extends NamedWriteable {

    default Object convert(Object value) {
        return convert(value, null);
    }

    /**
     * Converts `value` to the target type considering `zoneId` if needed.
     * `zoneId` can be null and it is ignored for all non-timezone aware conversions.
     */
    Object convert(Object value, ZoneId zoneId);

}
