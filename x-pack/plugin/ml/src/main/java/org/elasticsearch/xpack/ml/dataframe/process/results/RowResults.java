/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class RowResults implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("row_results");
    public static final ParseField CHECKSUM = new ParseField("checksum");
    public static final ParseField RESULTS = new ParseField("results");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RowResults, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new RowResults((Integer) a[0], (Map<String, Object>) a[1]));

    static {
        PARSER.declareInt(constructorArg(), CHECKSUM);
        PARSER.declareObject(constructorArg(), (p, context) -> p.map(), RESULTS);
    }

    private final int checksum;
    private final Map<String, Object> results;

    public RowResults(int checksum, Map<String, Object> results) {
        this.checksum = Objects.requireNonNull(checksum);
        this.results = Objects.requireNonNull(results);
    }

    public int getChecksum() {
        return checksum;
    }

    public Map<String, Object> getResults() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CHECKSUM.getPreferredName(), checksum);
        builder.field(RESULTS.getPreferredName(), results);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        RowResults that = (RowResults) other;
        return checksum == that.checksum && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checksum, results);
    }
}
