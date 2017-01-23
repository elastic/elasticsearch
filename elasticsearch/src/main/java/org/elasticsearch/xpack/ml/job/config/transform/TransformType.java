/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Enum type representing the different transform functions
 * with functions for converting between the enum and its
 * pretty name i.e. human readable string.
 */
public enum TransformType implements ToXContent, Writeable {
    // Name, arity, arguments, outputs, default output names, has condition
    DOMAIN_SPLIT(Names.DOMAIN_SPLIT_NAME, IntRange.singleton(1), IntRange.singleton(0),
            IntRange.closed(1, 2), Arrays.asList("subDomain", "hrd")),
    CONCAT(Names.CONCAT_NAME, IntRange.atLeast(2), IntRange.closed(0, 1), IntRange.singleton(1),
            Arrays.asList("concat")),
    REGEX_EXTRACT(Names.EXTRACT_NAME, IntRange.singleton(1), IntRange.singleton(1), IntRange.atLeast(1),
            Arrays.asList("extract"), false),
    REGEX_SPLIT(Names.SPLIT_NAME, IntRange.singleton(1), IntRange.singleton(1), IntRange.atLeast(1),
            Arrays.asList("split"), false),
    EXCLUDE(Names.EXCLUDE_NAME, IntRange.atLeast(1), IntRange.singleton(0), IntRange.singleton(0),
            Arrays.asList(), true),
    LOWERCASE(Names.LOWERCASE_NAME, IntRange.singleton(1), IntRange.singleton(0), IntRange.singleton(1),
            Arrays.asList("lowercase")),
    UPPERCASE(Names.UPPERCASE_NAME, IntRange.singleton(1), IntRange.singleton(0), IntRange.singleton(1),
            Arrays.asList("uppercase")),
    TRIM(Names.TRIM_NAME, IntRange.singleton(1), IntRange.singleton(0), IntRange.singleton(1),
            Arrays.asList("trim"));

    /**
     * Transform names.
     *
     * Enums cannot use static fields in their constructors as the
     * enum values are initialised before the statics.
     * Having the static fields in nested class means they are created
     * when required.
     */
    public class Names {
        public static final String DOMAIN_SPLIT_NAME = "domain_split";
        public static final String CONCAT_NAME = "concat";
        public static final String EXTRACT_NAME = "extract";
        public static final String SPLIT_NAME = "split";
        public static final String EXCLUDE_NAME = "exclude";
        public static final String LOWERCASE_NAME = "lowercase";
        public static final String UPPERCASE_NAME = "uppercase";
        public static final String TRIM_NAME = "trim";

        private Names() {
        }
    }

    private final IntRange arityRange;
    private final IntRange argumentsRange;
    private final IntRange outputsRange;
    private final String prettyName;
    private final List<String> defaultOutputNames;
    private final boolean hasCondition;

    TransformType(String prettyName, IntRange arityIntRange,
            IntRange argumentsIntRange, IntRange outputsIntRange,
            List<String> defaultOutputNames) {
        this(prettyName, arityIntRange, argumentsIntRange, outputsIntRange, defaultOutputNames, false);
    }

    TransformType(String prettyName, IntRange arityIntRange,
            IntRange argumentsIntRange, IntRange outputsIntRange,
            List<String> defaultOutputNames, boolean hasCondition) {
        this.arityRange = arityIntRange;
        this.argumentsRange = argumentsIntRange;
        this.outputsRange = outputsIntRange;
        this.prettyName = prettyName;
        this.defaultOutputNames = defaultOutputNames;
        this.hasCondition = hasCondition;
    }

    /**
     * The count IntRange of inputs the transform expects.
     */
    public IntRange arityRange() {
        return this.arityRange;
    }

    /**
     * The count IntRange of arguments the transform expects.
     */
    public IntRange argumentsRange() {
        return this.argumentsRange;
    }

    /**
     * The count IntRange of outputs the transform expects.
     */
    public IntRange outputsRange() {
        return this.outputsRange;
    }

    public String prettyName() {
        return prettyName;
    }

    public List<String> defaultOutputNames() {
        return defaultOutputNames;
    }

    public boolean hasCondition() {
        return hasCondition;
    }

    @Override
    public String toString() {
        return prettyName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(ordinal());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(prettyName);
        return builder;
    }

    /**
     * Get the enum for the given pretty name.
     * The static function valueOf() cannot be overridden so use
     * this method instead when converting from the pretty name
     * to enum.
     */
    public static TransformType fromString(String prettyName) throws IllegalArgumentException {
        Set<TransformType> all = EnumSet.allOf(TransformType.class);

        for (TransformType type : all) {
            if (type.prettyName().equals(prettyName)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown [transformType]: [" + prettyName + "]");
    }

}
