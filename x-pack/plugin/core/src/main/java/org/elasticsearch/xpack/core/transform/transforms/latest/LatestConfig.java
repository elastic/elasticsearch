/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.latest;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class LatestConfig implements Writeable, ToXContentObject {

    private static final String NAME = "latest_config";

    private static final ParseField UNIQUE_KEY = new ParseField("unique_key");
    private static final ParseField SORT = new ParseField("sort");

    private final List<String> uniqueKey;
    private final String sort;

    private static final ConstructingObjectParser<LatestConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<LatestConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LatestConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<LatestConfig, Void> parser =
            new ConstructingObjectParser<>(NAME, lenient, args -> new LatestConfig((List<String>) args[0], (String) args[1]));

        parser.declareStringArray(constructorArg(), UNIQUE_KEY);
        parser.declareString(constructorArg(), SORT);

        return parser;
    }

    public static LatestConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    public LatestConfig(List<String> uniqueKey, String sort) {
        this.uniqueKey = ExceptionsHelper.requireNonNull(uniqueKey, UNIQUE_KEY.getPreferredName());
        this.sort = ExceptionsHelper.requireNonNull(sort, SORT.getPreferredName());
    }

    public LatestConfig(StreamInput in) throws IOException {
        this.uniqueKey = in.readStringList();
        this.sort = in.readString();
    }

    public List<String> getUniqueKey() {
        return uniqueKey;
    }

    public String getSort() {
        return sort;
    }

    public List<SortBuilder<?>> getSorts() {
        return Collections.singletonList(SortBuilders.fieldSort(sort).order(SortOrder.DESC));
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (uniqueKey.isEmpty()) {
            validationException = addValidationError("latest.unique_key must be non-empty", validationException);
        } else {
            Set<String> uniqueKeyElements = new HashSet<>();
            for (int i = 0; i < uniqueKey.size(); ++i) {
                if (uniqueKey.get(i).isEmpty()) {
                    validationException =
                        addValidationError("latest.unique_key[" + i + "] element must be non-empty", validationException);
                } else if (uniqueKeyElements.contains(uniqueKey.get(i))) {
                    validationException =
                        addValidationError(
                            "latest.unique_key elements must be unique, found duplicate element [" + uniqueKey.get(i) + "]",
                            validationException);
                } else {
                    uniqueKeyElements.add(uniqueKey.get(i));
                }
            }
        }

        if (sort.isEmpty()) {
            validationException = addValidationError("latest.sort must be non-empty", validationException);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(uniqueKey);
        out.writeString(sort);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        LatestConfig that = (LatestConfig) other;
        return Objects.equals(this.uniqueKey, that.uniqueKey) && Objects.equals(this.sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueKey, sort);
    }
}
