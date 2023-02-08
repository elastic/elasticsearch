/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public abstract class RankResultContext implements Writeable {

    public static RankResultContext readFrom(StreamInput in) throws IOException {
        String name = in.readString();

        return null;
    }

    public abstract String getName();

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(getName());
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;
}
