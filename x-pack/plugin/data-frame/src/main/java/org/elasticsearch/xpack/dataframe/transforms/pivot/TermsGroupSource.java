/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class TermsGroupSource extends SingleGroupSource<TermsGroupSource> {
    private static final String NAME = "data_frame_terms_group";

    private static final ConstructingObjectParser<TermsGroupSource, Void> PARSER = createParser(false);
    private static final ConstructingObjectParser<TermsGroupSource, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<TermsGroupSource, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<TermsGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, ignoreUnknownFields, (args) -> {
            String field = (String) args[0];
            return new TermsGroupSource(field);
        });

        SingleGroupSource.declareValuesSourceFields(parser, null);
        return parser;
    }

    public TermsGroupSource(String field) {
        super(field);
    }

    public TermsGroupSource(StreamInput in) throws IOException {
        super(in);
    }

    public static TermsGroupSource fromXContent(final XContentParser parser, boolean ignoreUnknownFields) throws IOException {
        if (ignoreUnknownFields) {
            return LENIENT_PARSER.apply(parser, null);
        }
        // else
        return PARSER.apply(parser, null);
    }
}
