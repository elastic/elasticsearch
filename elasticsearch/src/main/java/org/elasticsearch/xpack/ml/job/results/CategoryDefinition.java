/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class CategoryDefinition extends ToXContentToBytes implements Writeable {

    public static final ParseField TYPE = new ParseField("category_definition");
    public static final ParseField CATEGORY_ID = new ParseField("category_id");
    public static final ParseField TERMS = new ParseField("terms");
    public static final ParseField REGEX = new ParseField("regex");
    public static final ParseField MAX_MATCHING_LENGTH = new ParseField("max_matching_length");
    public static final ParseField EXAMPLES = new ParseField("examples");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("categories");

    public static final ConstructingObjectParser<CategoryDefinition, Void> PARSER =
            new ConstructingObjectParser<>(TYPE.getPreferredName(), a -> new CategoryDefinition((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(CategoryDefinition::setCategoryId, CATEGORY_ID);
        PARSER.declareString(CategoryDefinition::setTerms, TERMS);
        PARSER.declareString(CategoryDefinition::setRegex, REGEX);
        PARSER.declareLong(CategoryDefinition::setMaxMatchingLength, MAX_MATCHING_LENGTH);
        PARSER.declareStringArray(CategoryDefinition::setExamples, EXAMPLES);
    }

    public static String documentId(String jobId, String categoryId) {
        return jobId + "-" + categoryId;
    }

    private final String jobId;
    private long id = 0L;
    private String terms = "";
    private String regex = "";
    private long maxMatchingLength = 0L;
    private final Set<String> examples;

    public CategoryDefinition(String jobId) {
        this.jobId = jobId;
        examples = new TreeSet<>();
    }

    public CategoryDefinition(StreamInput in) throws IOException {
        jobId = in.readString();
        id = in.readLong();
        terms = in.readString();
        regex = in.readString();
        maxMatchingLength = in.readLong();
        examples = new TreeSet<>(in.readList(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(id);
        out.writeString(terms);
        out.writeString(regex);
        out.writeLong(maxMatchingLength);
        out.writeStringList(new ArrayList<>(examples));
    }

    public String getJobId() {
        return jobId;
    }

    public long getCategoryId() {
        return id;
    }

    public void setCategoryId(long categoryId) {
        id = categoryId;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(CATEGORY_ID.getPreferredName(), id);
        builder.field(TERMS.getPreferredName(), terms);
        builder.field(REGEX.getPreferredName(), regex);
        builder.field(MAX_MATCHING_LENGTH.getPreferredName(), maxMatchingLength);
        builder.field(EXAMPLES.getPreferredName(), examples);
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
                && Objects.equals(this.id, that.id)
                && Objects.equals(this.terms, that.terms)
                && Objects.equals(this.regex, that.regex)
                && Objects.equals(this.maxMatchingLength, that.maxMatchingLength)
                && Objects.equals(this.examples, that.examples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, id, terms, regex, maxMatchingLength, examples);
    }
}
