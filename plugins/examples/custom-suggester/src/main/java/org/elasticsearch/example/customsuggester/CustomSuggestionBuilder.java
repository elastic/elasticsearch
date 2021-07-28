/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.customsuggester;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CustomSuggestionBuilder extends SuggestionBuilder<CustomSuggestionBuilder> {

    public static final String SUGGESTION_NAME = "custom";

    protected static final ParseField RANDOM_SUFFIX_FIELD = new ParseField("suffix");

    private String randomSuffix;

    public CustomSuggestionBuilder(String randomField, String randomSuffix) {
        super(randomField);
        this.randomSuffix = randomSuffix;
    }

    /**
     * Read from a stream.
     */
    public CustomSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        this.randomSuffix = in.readString();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(randomSuffix);
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RANDOM_SUFFIX_FIELD.getPreferredName(), randomSuffix);
        return builder;
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(CustomSuggestionBuilder other) {
        return Objects.equals(randomSuffix, other.randomSuffix);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(randomSuffix);
    }

    public static CustomSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String fieldname = null;
        String suffix = null;
        String analyzer = null;
        int sizeField = -1;
        int shardSize = -1;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SuggestionBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzer = parser.text();
                } else if (SuggestionBuilder.FIELDNAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fieldname = parser.text();
                } else if (SuggestionBuilder.SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    sizeField = parser.intValue();
                } else if (SuggestionBuilder.SHARDSIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    shardSize = parser.intValue();
                } else if (RANDOM_SUFFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    suffix = parser.text();
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                    "suggester[custom] doesn't support field [" + currentFieldName + "]");
            }
        }

        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (fieldname == null) {
            throw new ParsingException(parser.getTokenLocation(), "the required field option is missing");
        }
        CustomSuggestionBuilder builder = new CustomSuggestionBuilder(fieldname, suffix);
        if (analyzer != null) {
            builder.analyzer(analyzer);
        }
        if (sizeField != -1) {
            builder.size(sizeField);
        }
        if (shardSize != -1) {
            builder.shardSize(shardSize);
        }
        return builder;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext build(SearchExecutionContext context) throws IOException {
        Map<String, Object> options = new HashMap<>();
        options.put(FIELDNAME_FIELD.getPreferredName(), field());
        options.put(RANDOM_SUFFIX_FIELD.getPreferredName(), randomSuffix);
        CustomSuggestionContext customSuggestionsContext = new CustomSuggestionContext(context, options);
        customSuggestionsContext.setField(field());
        assert text != null;
        customSuggestionsContext.setText(BytesRefs.toBytesRef(text));
        return customSuggestionsContext;
    }

}
