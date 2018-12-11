/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.analytics.process;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class AnalyticsResult implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("analytics_result");
    public static final ParseField ID_HASH = new ParseField("id_hash");
    public static final ParseField RESULTS = new ParseField("results");

    static final ConstructingObjectParser<AnalyticsResult, Void> PARSER = new ConstructingObjectParser<>(TYPE.getPreferredName(),
            a -> new AnalyticsResult((String) a[0], (Map<String, Object>) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_HASH);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, context) -> p.map(), RESULTS);
    }

    private final String idHash;
    private final Map<String, Object> results;

    public AnalyticsResult(String idHash, Map<String, Object> results) {
        this.idHash = Objects.requireNonNull(idHash);
        this.results = Objects.requireNonNull(results);
    }

    public String getIdHash() {
        return idHash;
    }

    public Map<String, Object> getResults() {
        return results;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_HASH.getPreferredName(), idHash);
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

        AnalyticsResult that = (AnalyticsResult) other;
        return Objects.equals(idHash, that.idHash) && Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idHash, results);
    }
}
