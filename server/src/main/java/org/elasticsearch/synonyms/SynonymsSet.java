/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// TODO review the need for this class, we might use just SynonymRule as this is a just a holder of SynonymRule
public class SynonymsSet implements Writeable, ToXContentObject {

    public static final ParseField SYNONYMS_SET_FIELD = new ParseField("synonyms_set");
    private static final ConstructingObjectParser<SynonymsSet, Void> PARSER = new ConstructingObjectParser<>("synonyms_set", args -> {
        @SuppressWarnings("unchecked")
        final List<SynonymRule> synonyms = (List<SynonymRule>) args[0];
        return new SynonymsSet(synonyms.toArray(new SynonymRule[synonyms.size()]));
    });

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> SynonymRule.fromXContent(p), SYNONYMS_SET_FIELD);
    }

    private final SynonymRule[] synonyms;

    public SynonymsSet(SynonymRule[] synonyms) {
        Objects.requireNonNull(synonyms, "synonyms cannot be null");
        this.synonyms = synonyms;
    }

    public SynonymsSet(StreamInput in) throws IOException {
        this.synonyms = in.readArray(SynonymRule::new, SynonymRule[]::new);
    }

    public SynonymRule[] synonyms() {
        return synonyms;
    }

    public static SynonymsSet fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.array(SYNONYMS_SET_FIELD.getPreferredName(), (Object[]) synonyms);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(synonyms);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SynonymsSet that = (SynonymsSet) o;
        return Arrays.equals(synonyms, that.synonyms);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(synonyms);
    }
}
