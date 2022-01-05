/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.core.TimeValue;

/**
 * Utility class that handles conversion of classes in sql-proto (without any Elasticsearch dependencies)
 * into sql-action which do depend on Elasticsearch (since they are meant to be used in this environment).
 */
final class ProtoShim {

    private ProtoShim() {}

    static TimeValue fromProto(org.elasticsearch.xpack.sql.proto.core.TimeValue protoTimeValue) {
        if (protoTimeValue == null) {
            return null;
        }
        return new TimeValue(protoTimeValue.duration(), protoTimeValue.timeUnit());
    }

    static org.elasticsearch.xpack.sql.proto.core.TimeValue toProto(TimeValue timeValue) {
        if (timeValue == null) {
            return null;
        }
        return new org.elasticsearch.xpack.sql.proto.core.TimeValue(timeValue.duration(), timeValue.timeUnit());
    }
}
