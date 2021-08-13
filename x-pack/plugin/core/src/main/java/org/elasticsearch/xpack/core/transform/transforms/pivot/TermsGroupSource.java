/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/*
 * A terms aggregation source for group_by
 */
public class TermsGroupSource extends SingleGroupSource {
    private static final String NAME = "data_frame_terms_group";

    private static final ConstructingObjectParser<TermsGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<TermsGroupSource, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TermsGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<TermsGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            ScriptConfig scriptConfig = (ScriptConfig) args[1];
            boolean missingBucket = args[2] == null ? false : (boolean) args[2];

            return new TermsGroupSource(field, scriptConfig, missingBucket);
        });

        SingleGroupSource.declareValuesSourceFields(parser, lenient);
        return parser;
    }

    public TermsGroupSource(final String field, final ScriptConfig scriptConfig, boolean missingBucket) {
        super(field, scriptConfig, missingBucket);
    }

    public TermsGroupSource(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Type getType() {
        return Type.TERMS;
    }

    public static TermsGroupSource fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
