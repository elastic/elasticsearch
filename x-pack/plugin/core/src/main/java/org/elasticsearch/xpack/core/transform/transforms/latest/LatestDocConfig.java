/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class LatestDocConfig implements Writeable, ToXContentObject {

    private static final String NAME = "transform_latest_doc";

    private static final ParseField UNIQUE_KEY = new ParseField("unique_key");
    private static final ParseField SORT = new ParseField("sort");

    private final List<String> uniqueKey;
    private final List<SortBuilder<?>> sort;

    private static final ConstructingObjectParser<LatestDocConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<LatestDocConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<LatestDocConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<LatestDocConfig, Void> parser =
            new ConstructingObjectParser<>(
                NAME, lenient, args -> new LatestDocConfig((List<String>) args[0], (List<SortBuilder<?>>) args[1]));

        parser.declareStringArray(constructorArg(), UNIQUE_KEY);
        parser.declareField(constructorArg(), (p, c) -> SortBuilder.fromXContent(p), SORT, ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);

        return parser;
    }

    public static LatestDocConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public LatestDocConfig(List<String> uniqueKey, List<SortBuilder<?>> sort) {
        this.uniqueKey = ExceptionsHelper.requireNonNull(uniqueKey, UNIQUE_KEY.getPreferredName());
        this.sort = ExceptionsHelper.requireNonNull(sort, SORT.getPreferredName());
    }

    public LatestDocConfig(StreamInput in) throws IOException {
        this.uniqueKey = in.readStringList();
        this.sort = in.readList(FieldSortBuilder::new);
    }

    public List<String> getUniqueKey() {
        return uniqueKey;
    }

    public List<SortBuilder<?>> getSort() {
        return sort;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (uniqueKey.isEmpty()) {
            validationException = addValidationError("latest_doc.unique_key must be non-empty", validationException);
        } else {
            for (int i = 0; i < uniqueKey.size(); ++i) {
                if (Strings.isNullOrEmpty(uniqueKey.get(i))) {
                    validationException =
                        addValidationError("latest_doc.unique_key[" + i + "] element must be non-empty", validationException);
                }
            }
        }

        if (sort.size() != 1) {
            validationException = addValidationError("latest_doc.sort must have exactly one element", validationException);
        } else {
            SortBuilder<?> theOnlySort = sort.get(0);
            if (theOnlySort instanceof FieldSortBuilder == false) {
                validationException =
                    addValidationError(
                        "latest_doc.sort[0] must be sorting based on a document field, was: " + theOnlySort.getWriteableName(),
                        validationException);
            }
        }

        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(UNIQUE_KEY.getPreferredName(), uniqueKey);
        builder.field(SORT.getPreferredName(), sort);
        builder.endObject();
        return builder;
    }

    public void toCompositeAggXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());

        builder.startArray();
        for (String field : uniqueKey) {
            builder.startObject();
            builder.startObject(field);
            builder.startObject(TermsAggregationBuilder.NAME);
            builder.field("field", field);
            builder.endObject();
            builder.endObject();
            builder.endObject();
        }
        builder.endArray();

        builder.endObject(); // unique_key
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(uniqueKey);
        out.writeList(sort);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        LatestDocConfig that = (LatestDocConfig) other;
        return Objects.equals(this.uniqueKey, that.uniqueKey) && Objects.equals(this.sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueKey, sort);
    }
}
