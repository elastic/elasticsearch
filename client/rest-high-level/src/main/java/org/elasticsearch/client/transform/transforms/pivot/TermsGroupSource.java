/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TermsGroupSource extends SingleGroupSource implements ToXContentObject {

    private static final ConstructingObjectParser<TermsGroupSource, Void> PARSER = new ConstructingObjectParser<>(
        "terms_group_source",
        true,
        args -> new TermsGroupSource((String) args[0], (Script) args[1], args[2] == null ? false : (boolean) args[2])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        Script.declareScript(PARSER, optionalConstructorArg(), SCRIPT);
        PARSER.declareBoolean(optionalConstructorArg(), MISSING_BUCKET);
    }

    public static TermsGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    TermsGroupSource(final String field, final Script script) {
        this(field, script, false);
    }

    TermsGroupSource(final String field, final Script script, final boolean missingBucket) {
        super(field, script, missingBucket);
    }

    @Override
    public Type getType() {
        return Type.TERMS;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        super.innerXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String field;
        private Script script;
        private boolean missingBucket;

        /**
         * The field with which to construct the terms grouping
         * @param field The field name
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * The script with which to construct the terms grouping
         * @param script The script
         * @return The {@link Builder} with the script set.
         */
        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        /**
         * Sets the value of "missing_bucket"
         * @param missingBucket value of "missing_bucket" to be set
         * @return The {@link Builder} with "missing_bucket" set.
         */
        public Builder setMissingBucket(boolean missingBucket) {
            this.missingBucket = missingBucket;
            return this;
        }

        public TermsGroupSource build() {
            return new TermsGroupSource(field, script, missingBucket);
        }
    }
}
