/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A file stored inside a blob, with its length. It is not part of {@link StatelessCompoundCommit} because blobs other than compound
 * commits store files too. Its natural order is by length and then by name, and its XContent encoding is persisted in the header of
 * compound commits.
 */
record InternalFile(String name, long length) implements Writeable, ToXContentObject, Comparable<InternalFile> {

    static final ConstructingObjectParser<InternalFile, Void> PARSER = new ConstructingObjectParser<>(
        "internal_file",
        true,
        args -> new InternalFile((String) args[0], (long) args[1])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField("name"));
        PARSER.declareLong(constructorArg(), new ParseField("length"));
    }

    InternalFile(StreamInput streamInput) throws IOException {
        this(streamInput.readString(), streamInput.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeLong(length);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("name", name).field("length", length).endObject();
    }

    @Override
    public int compareTo(InternalFile o) {
        int cmp = Long.compare(length, o.length);
        if (cmp != 0) {
            return cmp;
        }
        return name.compareTo(o.name);
    }
}
