/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.xpack.sql.proto.content.ContentLocation;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;

/**
 * Utility class that handles conversion of classes in sql-proto (without any Elasticsearch dependencies)
 * into sql-action which do depend on Elasticsearch (since they are meant to be used in this environment).
 */
final class ProtoShim {

    private ProtoShim() {}

    //
    // Core classes
    //
    static org.elasticsearch.core.TimeValue fromProto(TimeValue fromProto) {
        if (fromProto == null) {
            return null;
        }
        return new org.elasticsearch.core.TimeValue(fromProto.duration(), fromProto.timeUnit());
    }

    static TimeValue toProto(org.elasticsearch.core.TimeValue toProto) {
        if (toProto == null) {
            return null;
        }
        return new TimeValue(toProto.duration(), toProto.timeUnit());
    }

    //
    // XContent classes
    //
    static org.elasticsearch.xcontent.XContentLocation fromProto(ContentLocation fromProto) {
        if (fromProto == null) {
            return null;
        }
        return new org.elasticsearch.xcontent.XContentLocation(fromProto.lineNumber, fromProto.columnNumber);
    }

    static ContentLocation toProto(org.elasticsearch.xcontent.XContentLocation toProto) {
        if (toProto == null) {
            return null;
        }
        return new ContentLocation(toProto.lineNumber(), toProto.columnNumber());
    }
}
