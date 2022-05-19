/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class VocabularyConfig implements ToXContentObject, Writeable {

    private static final ParseField INDEX = new ParseField("index");

    public static String docId(String modelId) {
        return modelId + "_vocabulary";
    }

    private static final ConstructingObjectParser<VocabularyConfig, Void> PARSER = new ConstructingObjectParser<>(
        "vocabulary_config",
        true,
        a -> new VocabularyConfig((String) a[0])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
    }

    // VocabularyConfig is not settable via the end user, so only the parser for reading from stored configurations is allowed
    public static VocabularyConfig fromXContentLenient(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String index;

    public VocabularyConfig(String index) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
    }

    public VocabularyConfig(StreamInput in) throws IOException {
        index = in.readString();
    }

    public String getIndex() {
        return index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VocabularyConfig that = (VocabularyConfig) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
