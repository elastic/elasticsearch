/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public final class NamedXContentObjectHelper {

    private NamedXContentObjectHelper() {}

    public static XContentBuilder writeNamedObjects(XContentBuilder builder,
                                                    ToXContent.Params params,
                                                    boolean useExplicitOrder,
                                                    String namedObjectsName,
                                                    List<? extends NamedXContentObject> namedObjects) throws IOException {
        if (useExplicitOrder) {
            builder.startArray(namedObjectsName);
        } else {
            builder.startObject(namedObjectsName);
        }
        for (NamedXContentObject object : namedObjects) {
            if (useExplicitOrder) {
                builder.startObject();
            }
            builder.field(object.getName(), object, params);
            if (useExplicitOrder) {
                builder.endObject();
            }
        }
        if (useExplicitOrder) {
            builder.endArray();
        } else {
            builder.endObject();
        }
        return builder;
    }

    public static XContentBuilder writeNamedObject(XContentBuilder builder,
                                                   ToXContent.Params params,
                                                   String namedObjectName,
                                                   NamedXContentObject namedObject) throws IOException {
        builder.startObject(namedObjectName);
        builder.field(namedObject.getName(), namedObject, params);
        builder.endObject();
        return builder;
    }
}
