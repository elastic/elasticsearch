/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class CategoryDefinition implements ToXContentObject, Writeable {

    /**
     * Legacy type, now used only as a discriminant in the document ID
     */
    public static final ParseField TYPE = new ParseField("category_definition");

    public static final ParseField CATEGORY_ID = new ParseField("category_id");
    public static final ParseField TERMS = new ParseField("terms");
    public static final ParseField REGEX = new ParseField("regex");
    public static final ParseField MAX_MATCHING_LENGTH = new ParseField("max_matching_length");
    public static final ParseField EXAMPLES = new ParseField("examples");
    public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("categories");

    public static final ConstructingObjectParser<CategoryDefinition, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<CategoryDefinition, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<CategoryDefinition, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CategoryDefinition, Void> parser = new ConstructingObjectParser<>(TYPE.getPreferredName(),
                ignoreUnknownFields, a -> new CategoryDefinition((String) a[0]));

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareLong(CategoryDefinition::setCategoryId, CATEGORY_ID);
        parser.declareString(CategoryDefinition::setTerms, TERMS);
        parser.declareString(CategoryDefinition::setRegex, REGEX);
        parser.declareLong(CategoryDefinition::setMaxMatchingLength, MAX_MATCHING_LENGTH);
        parser.declareStringArray(CategoryDefinition::setExamples, EXAMPLES);
        parser.declareString(CategoryDefinition::setGrokPattern, GROK_PATTERN);

        return parser;
    }

    private final String jobId;
    private long categoryId = 0L;
    private String terms = "";
    private String regex = "";
    private long maxMatchingLength = 0L;
    private final Set<String> examples;
    private String grokPattern;

    public CategoryDefinition(String jobId) {
        this.jobId = jobId;
        examples = new TreeSet<>();
    }

    public CategoryDefinition(StreamInput in) throws IOException {
        jobId = in.readString();
        categoryId = in.readLong();
        terms = in.readString();
        regex = in.readString();
        maxMatchingLength = in.readLong();
        examples = new TreeSet<>(in.readStringList());
        grokPattern = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(categoryId);
        out.writeString(terms);
        out.writeString(regex);
        out.writeLong(maxMatchingLength);
        out.writeStringCollection(examples);
        out.writeOptionalString(grokPattern);
    }

    public String getJobId() {
        return jobId;
    }

    public String getId() {
        return jobId + "_" + TYPE + "_" + categoryId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getTerms() {
        return terms;
    }

    public void setTerms(String terms) {
        this.terms = terms;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public long getMaxMatchingLength() {
        return maxMatchingLength;
    }

    public void setMaxMatchingLength(long maxMatchingLength) {
        this.maxMatchingLength = maxMatchingLength;
    }

    public List<String> getExamples() {
        return new ArrayList<>(examples);
    }

    public void setExamples(Collection<String> examples) {
        this.examples.clear();
        this.examples.addAll(examples);
    }

    public void addExample(String example) {
        examples.add(example);
    }

    public String getGrokPattern() {
        return grokPattern;
    }

    public void setGrokPattern(String grokPattern) {
        this.grokPattern = grokPattern;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(CATEGORY_ID.getPreferredName(), categoryId);
        builder.field(TERMS.getPreferredName(), terms);
        builder.field(REGEX.getPreferredName(), regex);
        builder.field(MAX_MATCHING_LENGTH.getPreferredName(), maxMatchingLength);
        builder.field(EXAMPLES.getPreferredName(), examples);
        if (grokPattern != null) {
            builder.field(GROK_PATTERN.getPreferredName(), grokPattern);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof CategoryDefinition == false) {
            return false;
        }
        CategoryDefinition that = (CategoryDefinition) other;
        return Objects.equals(this.jobId, that.jobId)
                && Objects.equals(this.categoryId, that.categoryId)
                && Objects.equals(this.terms, that.terms)
                && Objects.equals(this.regex, that.regex)
                && Objects.equals(this.maxMatchingLength, that.maxMatchingLength)
                && Objects.equals(this.examples, that.examples)
                && Objects.equals(this.grokPattern, that.grokPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, categoryId, terms, regex, maxMatchingLength, examples, grokPattern);
    }
}
