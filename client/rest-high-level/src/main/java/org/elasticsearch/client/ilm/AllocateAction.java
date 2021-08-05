/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class AllocateAction implements LifecycleAction, ToXContentObject {

    public static final String NAME = "allocate";
    static final ParseField NUMBER_OF_REPLICAS_FIELD = new ParseField("number_of_replicas");
    static final ParseField INCLUDE_FIELD = new ParseField("include");
    static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    static final ParseField REQUIRE_FIELD = new ParseField("require");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AllocateAction, Void> PARSER = new ConstructingObjectParser<>(NAME, true,
            a -> new AllocateAction((Integer) a[0], (Map<String, String>) a[1], (Map<String, String>) a[2], (Map<String, String>) a[3]));

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_REPLICAS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), INCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), EXCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), REQUIRE_FIELD);
    }

    private final Integer numberOfReplicas;
    private final Map<String, String> include;
    private final Map<String, String> exclude;
    private final Map<String, String> require;

    public static AllocateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public AllocateAction(Integer numberOfReplicas, Map<String, String> include, Map<String, String> exclude, Map<String, String> require) {
        if (include == null) {
            this.include = Collections.emptyMap();
        } else {
            this.include = include;
        }
        if (exclude == null) {
            this.exclude = Collections.emptyMap();
        } else {
            this.exclude = exclude;
        }
        if (require == null) {
            this.require = Collections.emptyMap();
        } else {
            this.require = require;
        }
        if (this.include.isEmpty() && this.exclude.isEmpty() && this.require.isEmpty() && numberOfReplicas == null) {
            throw new IllegalArgumentException(
                    "At least one of " + INCLUDE_FIELD.getPreferredName() + ", " + EXCLUDE_FIELD.getPreferredName() + " or "
                            + REQUIRE_FIELD.getPreferredName() + "must contain attributes for action " + NAME);
        }
        if (numberOfReplicas != null && numberOfReplicas < 0) {
            throw new IllegalArgumentException("[" + NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0");
        }
        this.numberOfReplicas = numberOfReplicas;
    }

    public Integer getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Map<String, String> getInclude() {
        return include;
    }

    public Map<String, String> getExclude() {
        return exclude;
    }

    public Map<String, String> getRequire() {
        return require;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (numberOfReplicas != null) {
            builder.field(NUMBER_OF_REPLICAS_FIELD.getPreferredName(), numberOfReplicas);
        }
        builder.field(INCLUDE_FIELD.getPreferredName(), include);
        builder.field(EXCLUDE_FIELD.getPreferredName(), exclude);
        builder.field(REQUIRE_FIELD.getPreferredName(), require);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfReplicas, include, exclude, require);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AllocateAction other = (AllocateAction) obj;
        return Objects.equals(numberOfReplicas, other.numberOfReplicas) &&
            Objects.equals(include, other.include) &&
            Objects.equals(exclude, other.exclude) &&
            Objects.equals(require, other.require);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
