/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PivotConfig implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_pivot";
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(PivotConfig.class);

    private final GroupConfig groups;
    private final AggregationConfig aggregationConfig;
    private final Integer maxPageSearchSize;

    private static final ConstructingObjectParser<PivotConfig, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<PivotConfig, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<PivotConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<PivotConfig, Void> parser = new ConstructingObjectParser<>(NAME, lenient, args -> {
            GroupConfig groups = (GroupConfig) args[0];

            // allow "aggs" and "aggregations" but require one to be specified
            // if somebody specifies both: throw
            AggregationConfig aggregationConfig = null;
            if (args[1] != null) {
                aggregationConfig = (AggregationConfig) args[1];
            }

            if (args[2] != null) {
                if (aggregationConfig != null) {
                    throw new IllegalArgumentException("Found two aggregation definitions: [aggs] and [aggregations]");
                }
                aggregationConfig = (AggregationConfig) args[2];
            }
            if (aggregationConfig == null) {
                throw new IllegalArgumentException("Required [aggregations]");
            }

            return new PivotConfig(groups, aggregationConfig, (Integer) args[3]);
        });

        parser.declareObject(constructorArg(), (p, c) -> (GroupConfig.fromXContent(p, lenient)), TransformField.GROUP_BY);

        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), TransformField.AGGREGATIONS);
        parser.declareObject(optionalConstructorArg(), (p, c) -> AggregationConfig.fromXContent(p, lenient), TransformField.AGGS);
        parser.declareInt(optionalConstructorArg(), TransformField.MAX_PAGE_SEARCH_SIZE);

        return parser;
    }

    public PivotConfig(final GroupConfig groups, final AggregationConfig aggregationConfig, Integer maxPageSearchSize) {
        this.groups = ExceptionsHelper.requireNonNull(groups, TransformField.GROUP_BY.getPreferredName());
        this.aggregationConfig = ExceptionsHelper.requireNonNull(aggregationConfig, TransformField.AGGREGATIONS.getPreferredName());
        this.maxPageSearchSize = maxPageSearchSize;

        if (maxPageSearchSize != null) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(),
                TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED
            );
        }
    }

    public PivotConfig(StreamInput in) throws IOException {
        this.groups = new GroupConfig(in);
        this.aggregationConfig = new AggregationConfig(in);
        this.maxPageSearchSize = in.readOptionalInt();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.GROUP_BY.getPreferredName(), groups);
        builder.field(TransformField.AGGREGATIONS.getPreferredName(), aggregationConfig);
        if (maxPageSearchSize != null) {
            builder.field(TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(), maxPageSearchSize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        groups.writeTo(out);
        aggregationConfig.writeTo(out);
        out.writeOptionalInt(maxPageSearchSize);
    }

    public AggregationConfig getAggregationConfig() {
        return aggregationConfig;
    }

    public GroupConfig getGroupConfig() {
        return groups;
    }

    @Nullable
    public Integer getMaxPageSearchSize() {
        return maxPageSearchSize;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final PivotConfig that = (PivotConfig) other;

        return Objects.equals(this.groups, that.groups)
            && Objects.equals(this.aggregationConfig, that.aggregationConfig)
            && Objects.equals(this.maxPageSearchSize, that.maxPageSearchSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groups, aggregationConfig, maxPageSearchSize);
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {

        if (maxPageSearchSize != null && (maxPageSearchSize < 10 || maxPageSearchSize > MultiBucketConsumerService.DEFAULT_MAX_BUCKETS)) {
            validationException = addValidationError(
                "pivot.max_page_search_size ["
                    + maxPageSearchSize
                    + "] is out of range. The minimum value is 10 and the maximum is "
                    + MultiBucketConsumerService.DEFAULT_MAX_BUCKETS,
                validationException
            );
        }
        validationException = groups.validate(validationException);
        validationException = aggregationConfig.validate(validationException);

        List<String> usedNames = new ArrayList<>();
        usedNames.addAll(groups.getUsedNames());
        usedNames.addAll(aggregationConfig.getUsedNames());
        for (String failure : aggFieldValidation(usedNames)) {
            validationException = addValidationError(failure, validationException);
        }
        return validationException;
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {
        if (maxPageSearchSize != null) {
            onDeprecation.accept(
                // max_page_search_size got deprecated in 7.8, still accepted for 8.x, to be removed in 9.x
                new DeprecationIssue(
                    Level.CRITICAL,
                    "Transform [" + id + "] uses the deprecated setting [max_page_search_size]",
                    TransformDeprecations.MAX_PAGE_SEARCH_SIZE_BREAKING_CHANGES_URL,
                    TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED,
                    false,
                    null
                )
            );
        }

        groups.checkForDeprecations(id, namedXContentRegistry, onDeprecation);
        aggregationConfig.checkForDeprecations(id, namedXContentRegistry, onDeprecation);
    }

    public static PivotConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    /**
     * Does the following checks:
     *
     *  - determines if there are any full duplicate names between the aggregation names and the group by names.
     *  - finds if there are conflicting name paths that could cause a failure later when the config is started.
     *
     * Examples showing conflicting field name paths:
     *
     * aggName1: foo.bar.baz
     * aggName2: foo.bar
     *
     * This should fail as aggName1 will cause foo.bar to be an object, causing a conflict with the use of foo.bar in aggName2.
     * @param usedNames The aggregation and group_by names
     * @return List of validation failure messages
     */
    static List<String> aggFieldValidation(List<String> usedNames) {
        if (usedNames == null || usedNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> validationFailures = new ArrayList<>();

        usedNames.sort(String::compareTo);
        for (int i = 0; i < usedNames.size() - 1; i++) {
            if (usedNames.get(i + 1).startsWith(usedNames.get(i) + ".")) {
                validationFailures.add("field [" + usedNames.get(i) + "] cannot be both an object and a field");
            }
            if (usedNames.get(i + 1).equals(usedNames.get(i))) {
                validationFailures.add("duplicate field [" + usedNames.get(i) + "] detected");
            }
        }

        for (String name : usedNames) {
            if (name.startsWith(".")) {
                validationFailures.add("field [" + name + "] must not start with '.'");
            }
            if (name.endsWith(".")) {
                validationFailures.add("field [" + name + "] must not end with '.'");
            }
        }

        return validationFailures;
    }
}
