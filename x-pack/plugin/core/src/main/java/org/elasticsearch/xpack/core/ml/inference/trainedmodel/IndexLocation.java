/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class IndexLocation implements StrictlyParsedTrainedModelLocation, LenientlyParsedTrainedModelLocation {

    public static final ParseField INDEX = new ParseField("index");
    private static final ParseField NAME = new ParseField("name");

    private static final ConstructingObjectParser<IndexLocation, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<IndexLocation, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<IndexLocation, Void> createParser(boolean lenient) {
        ConstructingObjectParser<IndexLocation, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new IndexLocation((String) a[0])
        );
        parser.declareString(ConstructingObjectParser.constructorArg(), NAME);
        return parser;
    }

    public static IndexLocation fromXContentStrict(XContentParser parser) throws IOException {
        return STRICT_PARSER.parse(parser, null);
    }

    public static IndexLocation fromXContentLenient(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

    private final String indexName;

    public IndexLocation(String indexName) {
        this.indexName = Objects.requireNonNull(indexName);
    }

    public IndexLocation(StreamInput in) throws IOException {
        this.indexName = in.readString();
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getResourceName() {
        return getIndexName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), indexName);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
    }

    @Override
    public String getWriteableName() {
        return INDEX.getPreferredName();
    }

    @Override
    public String getName() {
        return INDEX.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexLocation that = (IndexLocation) o;
        return Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName);
    }
}
