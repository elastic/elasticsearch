/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.plugin.DummyQueryBuilder;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * Query simulating serialization error on versions earlier than CURRENT
 */
public class FailBeforeVersionQueryBuilder extends DummyQueryBuilder {

    public static final String NAME = "fail_before_current_version";
    private static Version previousMinor;

    static {
        List<Version> allVersions = VersionUtils.allVersions();
        for (int i = allVersions.size() - 1; i >= 0; i--) {
            Version v = allVersions.get(i);
            if (v.minor < Version.CURRENT.minor || v.major < Version.CURRENT.major) {
                previousMinor = v;
                break;
            }
        }
    }

    public FailBeforeVersionQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public FailBeforeVersionQueryBuilder() {}

    @Override
    protected void doWriteTo(StreamOutput out) {
        if (out.getVersion().onOrBefore(previousMinor)) {
            throw new IllegalArgumentException("This query isn't serializable to nodes on or before " + previousMinor);
        }
    }

    public static DummyQueryBuilder fromXContent(XContentParser parser) throws IOException {
        DummyQueryBuilder.fromXContent(parser);
        return new FailBeforeVersionQueryBuilder();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}