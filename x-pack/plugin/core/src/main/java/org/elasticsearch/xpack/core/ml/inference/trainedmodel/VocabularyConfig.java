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
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class VocabularyConfig implements ToXContentObject, Writeable {

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField ID = new ParseField("id");

    public static ConstructingObjectParser<VocabularyConfig, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<VocabularyConfig, Void> parser = new ConstructingObjectParser<>("vocabulary_config",
            ignoreUnknownFields, a -> new VocabularyConfig((String) a[0], (String) a[1]));
        parser.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        parser.declareString(ConstructingObjectParser.constructorArg(), ID);
        return parser;
    }

    private final String index;
    private final String id;

    public VocabularyConfig(String index, String id) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
        this.id = ExceptionsHelper.requireNonNull(id, ID);
    }

    public VocabularyConfig(StreamInput in) throws IOException {
        index = in.readString();
        id = in.readString();
    }

    public String getIndex() {
        return index;
    }

    public String getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.field(ID.getPreferredName(), id);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VocabularyConfig that = (VocabularyConfig) o;
        return Objects.equals(index, that.index) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id);
    }
}
