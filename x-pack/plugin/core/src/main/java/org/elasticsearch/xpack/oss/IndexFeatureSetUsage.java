/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.oss;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class IndexFeatureSetUsage extends XPackFeatureSet.Usage {

    private static Set<String> sort(Set<String> set) {
        return Collections.unmodifiableSet(new TreeSet<>(set));
    }

    private final Set<String> usedFieldTypes;
    private final Set<String> usedCharFilters, usedTokenizers, usedTokenFilters, usedAnalyzers;
    private final Set<String> usedBuiltInCharFilters, usedBuiltInTokenizers, usedBuiltInTokenFilters, usedBuiltInAnalyzers;

    public IndexFeatureSetUsage(Set<String> usedFieldTypes,
            Set<String> usedCharFilters, Set<String> usedTokenizers, Set<String> usedTokenFilters, Set<String> usedAnalyzers,
            Set<String> usedBuiltInCharFilters, Set<String> usedBuiltInTokenizers, Set<String> usedBuiltInTokenFilters,
            Set<String> usedBuiltInAnalyzers) {
        super(XPackField.INDEX, true, true);
        this.usedFieldTypes = sort(usedFieldTypes);
        this.usedCharFilters = sort(usedCharFilters);
        this.usedTokenizers = sort(usedTokenizers);
        this.usedTokenFilters = sort(usedTokenFilters);
        this.usedAnalyzers = sort(usedAnalyzers);
        this.usedBuiltInCharFilters = sort(usedBuiltInCharFilters);
        this.usedBuiltInTokenizers = sort(usedBuiltInTokenizers);
        this.usedBuiltInTokenFilters = sort(usedBuiltInTokenFilters);
        this.usedBuiltInAnalyzers = sort(usedBuiltInAnalyzers);
    }

    public IndexFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        usedFieldTypes = input.readSet(StreamInput::readString);
        usedCharFilters = input.readSet(StreamInput::readString);
        usedTokenizers = input.readSet(StreamInput::readString);
        usedTokenFilters = input.readSet(StreamInput::readString);
        usedAnalyzers = input.readSet(StreamInput::readString);
        usedBuiltInCharFilters = input.readSet(StreamInput::readString);
        usedBuiltInTokenizers = input.readSet(StreamInput::readString);
        usedBuiltInTokenFilters = input.readSet(StreamInput::readString);
        usedBuiltInAnalyzers = input.readSet(StreamInput::readString);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(usedFieldTypes, StreamOutput::writeString);
        out.writeCollection(usedCharFilters, StreamOutput::writeString);
        out.writeCollection(usedTokenizers, StreamOutput::writeString);
        out.writeCollection(usedTokenFilters, StreamOutput::writeString);
        out.writeCollection(usedAnalyzers, StreamOutput::writeString);
        out.writeCollection(usedBuiltInCharFilters, StreamOutput::writeString);
        out.writeCollection(usedBuiltInTokenizers, StreamOutput::writeString);
        out.writeCollection(usedBuiltInTokenFilters, StreamOutput::writeString);
        out.writeCollection(usedBuiltInAnalyzers, StreamOutput::writeString);
    }

    /**
     * Return the set of used field types in the cluster.
     */
    public Set<String> getUsedFieldTypes() {
        return usedFieldTypes;
    }

    /**
     * Return the set of used char filters in the cluster.
     */
    public Set<String> getUsedCharFilterTypes() {
        return usedCharFilters;
    }

    /**
     * Return the set of used tokenizers in the cluster.
     */
    public Set<String> getUsedTokenizerTypes() {
        return usedTokenizers;
    }

    /**
     * Return the set of used token filters in the cluster.
     */
    public Set<String> getUsedTokenFilterTypes() {
        return usedTokenFilters;
    }

    /**
     * Return the set of used analyzers in the cluster.
     */
    public Set<String> getUsedAnalyzerTypes() {
        return usedAnalyzers;
    }

    /**
     * Return the set of used built-in char filters in the cluster.
     */
    public Set<String> getUsedBuiltInCharFilters() {
        return usedBuiltInCharFilters;
    }

    /**
     * Return the set of used built-in tokenizers in the cluster.
     */
    public Set<String> getUsedBuiltInTokenizers() {
        return usedBuiltInTokenizers;
    }

    /**
     * Return the set of used built-in token filters in the cluster.
     */
    public Set<String> getUsedBuiltInTokenFilters() {
        return usedBuiltInTokenFilters;
    }

    /**
     * Return the set of used built-in analyzers in the cluster.
     */
    public Set<String> getUsedBuiltInAnalyzers() {
        return usedBuiltInAnalyzers;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);

        builder.startObject("analysis");
        {
            builder.field("char_filter_types", usedCharFilters);
            builder.field("tokenizer_types", usedTokenizers);
            builder.field("filter_types", usedTokenFilters);
            builder.field("analyzer_types", usedAnalyzers);

            builder.field("built_in_char_filters", usedBuiltInCharFilters);
            builder.field("built_in_tokenizers", usedBuiltInTokenizers);
            builder.field("built_in_filters", usedBuiltInTokenFilters);
            builder.field("built_in_analyzers", usedBuiltInAnalyzers);
        }
        builder.endObject();

        builder.startObject("mappings");
        {
            builder.field("field_types", usedFieldTypes);
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexFeatureSetUsage that = (IndexFeatureSetUsage) o;
        return available == that.available && enabled == that.enabled &&
                Objects.equals(usedFieldTypes, that.usedFieldTypes) &&
                Objects.equals(usedCharFilters, that.usedCharFilters) &&
                Objects.equals(usedTokenizers, that.usedTokenizers) &&
                Objects.equals(usedTokenFilters, that.usedTokenFilters) &&
                Objects.equals(usedAnalyzers, that.usedAnalyzers) &&
                Objects.equals(usedBuiltInCharFilters, that.usedBuiltInCharFilters) &&
                Objects.equals(usedBuiltInTokenizers, that.usedBuiltInTokenizers) &&
                Objects.equals(usedBuiltInTokenFilters, that.usedBuiltInTokenFilters) &&
                Objects.equals(usedBuiltInAnalyzers, that.usedBuiltInAnalyzers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, usedFieldTypes, usedCharFilters, usedTokenizers, usedTokenFilters,
                usedAnalyzers, usedBuiltInCharFilters, usedBuiltInTokenizers, usedBuiltInTokenFilters,
                usedBuiltInAnalyzers);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
