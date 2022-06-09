/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Holds stats about a mapped field.
 */
public class FieldStats extends IndexFeatureStats {
    int scriptCount = 0;
    final Set<String> scriptLangs;
    final FieldScriptStats fieldScriptStats;

    FieldStats(String name) {
        super(name);
        scriptLangs = new HashSet<>();
        fieldScriptStats = new FieldScriptStats();
    }

    FieldStats(StreamInput in) throws IOException {
        super(in);
        scriptCount = in.readVInt();
        scriptLangs = in.readSet(StreamInput::readString);
        fieldScriptStats = new FieldScriptStats(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(scriptCount);
        out.writeCollection(scriptLangs, StreamOutput::writeString);
        fieldScriptStats.writeTo(out);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script_count", scriptCount);
        if (scriptCount > 0) {
            builder.array("lang", scriptLangs.toArray(new String[0]));
            fieldScriptStats.toXContent(builder, params);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        FieldStats that = (FieldStats) o;
        return scriptCount == that.scriptCount && scriptLangs.equals(that.scriptLangs) && fieldScriptStats.equals(that.fieldScriptStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scriptCount, scriptLangs, fieldScriptStats);
    }
}
