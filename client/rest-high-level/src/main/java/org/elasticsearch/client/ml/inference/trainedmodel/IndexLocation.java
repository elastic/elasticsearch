/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class IndexLocation implements TrainedModelLocation {

    public static final String INDEX = "index";
    private static final ParseField NAME = new ParseField("name");

    private static final ConstructingObjectParser<IndexLocation, Void> PARSER =
        new ConstructingObjectParser<>(INDEX, true, a -> new IndexLocation((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
    }

    public static IndexLocation fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String index;

    public IndexLocation(String index) {
        this.index = Objects.requireNonNull(index);
    }

    public String getIndex() {
        return index;
    }

    @Override
    public String getName() {
        return INDEX;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME.getPreferredName(), index);
        builder.endObject();
        return builder;
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
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
