/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.io.IOException;
import java.util.Set;

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
            return new TermsGroupSource(field);
        });

        SingleGroupSource.declareValuesSourceFields(parser);
        return parser;
    }

    public TermsGroupSource(final String field) {
        super(field);
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

    @Override
    public QueryBuilder getIncrementalBucketUpdateFilterQuery(Set<String> changedBuckets) {
        return new TermsQueryBuilder(field, changedBuckets);
    }

    @Override
    public boolean supportsIncrementalBucketUpdate() {
        return true;
    }
}
