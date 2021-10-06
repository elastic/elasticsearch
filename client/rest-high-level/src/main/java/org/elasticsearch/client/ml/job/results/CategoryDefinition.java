/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class CategoryDefinition implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("category_definition");

    public static final ParseField CATEGORY_ID = new ParseField("category_id");
    public static final ParseField PARTITION_FIELD_NAME = new ParseField("partition_field_name");
    public static final ParseField PARTITION_FIELD_VALUE = new ParseField("partition_field_value");
    public static final ParseField TERMS = new ParseField("terms");
    public static final ParseField REGEX = new ParseField("regex");
    public static final ParseField MAX_MATCHING_LENGTH = new ParseField("max_matching_length");
    public static final ParseField EXAMPLES = new ParseField("examples");
    public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");
    public static final ParseField NUM_MATCHES = new ParseField("num_matches");
    public static final ParseField PREFERRED_TO_CATEGORIES = new ParseField("preferred_to_categories");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("categories");

    public static final ConstructingObjectParser<CategoryDefinition, Void> PARSER =
        new ConstructingObjectParser<>(TYPE.getPreferredName(), true, a -> new CategoryDefinition((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(CategoryDefinition::setCategoryId, CATEGORY_ID);
        PARSER.declareString(CategoryDefinition::setPartitionFieldName, PARTITION_FIELD_NAME);
        PARSER.declareString(CategoryDefinition::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        PARSER.declareString(CategoryDefinition::setTerms, TERMS);
        PARSER.declareString(CategoryDefinition::setRegex, REGEX);
        PARSER.declareLong(CategoryDefinition::setMaxMatchingLength, MAX_MATCHING_LENGTH);
        PARSER.declareStringArray(CategoryDefinition::setExamples, EXAMPLES);
        PARSER.declareString(CategoryDefinition::setGrokPattern, GROK_PATTERN);
        PARSER.declareLong(CategoryDefinition::setNumMatches, NUM_MATCHES);
        PARSER.declareLongArray(CategoryDefinition::setPreferredToCategories, PREFERRED_TO_CATEGORIES);
    }

    private final String jobId;
    private long categoryId = 0L;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String terms = "";
    private String regex = "";
    private long maxMatchingLength = 0L;
    private final Set<String> examples = new TreeSet<>();
    private String grokPattern;
    private long numMatches = 0L;
    private List<Long> preferredToCategories;

    CategoryDefinition(String jobId) {
        this.jobId = jobId;
    }

    public String getJobId() {
        return jobId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    public void setPartitionFieldName(String partitionFieldName) {
        this.partitionFieldName = partitionFieldName;
    }

    public String getPartitionFieldValue() {
        return partitionFieldValue;
    }

    public void setPartitionFieldValue(String partitionFieldValue) {
        this.partitionFieldValue = partitionFieldValue;
    }

    public String getTerms() {
        return terms;
    }

    void setTerms(String terms) {
        this.terms = terms;
    }

    public String getRegex() {
        return regex;
    }

    void setRegex(String regex) {
        this.regex = regex;
    }

    public long getMaxMatchingLength() {
        return maxMatchingLength;
    }

    void setMaxMatchingLength(long maxMatchingLength) {
        this.maxMatchingLength = maxMatchingLength;
    }

    public List<String> getExamples() {
        return new ArrayList<>(examples);
    }

    void setExamples(Collection<String> examples) {
        this.examples.clear();
        this.examples.addAll(examples);
    }

    void addExample(String example) {
        examples.add(example);
    }

    public String getGrokPattern() {
        return grokPattern;
    }

    void setGrokPattern(String grokPattern) {
        this.grokPattern = grokPattern;
    }

    public long getNumMatches() {
        return numMatches;
    }

    public void setNumMatches(long numMatches) {
        this.numMatches = numMatches;
    }

    public List<Long> getPreferredToCategories() {
        return preferredToCategories;
    }

    public void setPreferredToCategories(List<Long> preferredToCategories) {
        this.preferredToCategories = Collections.unmodifiableList(preferredToCategories);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(CATEGORY_ID.getPreferredName(), categoryId);
        if (partitionFieldName != null) {
            builder.field(PARTITION_FIELD_NAME.getPreferredName(), partitionFieldName);
        }
        if (partitionFieldValue != null) {
            builder.field(PARTITION_FIELD_VALUE.getPreferredName(), partitionFieldValue);
        }
        builder.field(TERMS.getPreferredName(), terms);
        builder.field(REGEX.getPreferredName(), regex);
        builder.field(MAX_MATCHING_LENGTH.getPreferredName(), maxMatchingLength);
        builder.field(EXAMPLES.getPreferredName(), examples);
        if (grokPattern != null) {
            builder.field(GROK_PATTERN.getPreferredName(), grokPattern);
        }
        builder.field(NUM_MATCHES.getPreferredName(), numMatches);
        if (preferredToCategories != null && (preferredToCategories.isEmpty() == false)) {
            builder.field(PREFERRED_TO_CATEGORIES.getPreferredName(), preferredToCategories);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        CategoryDefinition that = (CategoryDefinition) other;
        return Objects.equals(this.jobId, that.jobId)
            && Objects.equals(this.categoryId, that.categoryId)
            && Objects.equals(this.partitionFieldName, that.partitionFieldName)
            && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
            && Objects.equals(this.terms, that.terms)
            && Objects.equals(this.regex, that.regex)
            && Objects.equals(this.maxMatchingLength, that.maxMatchingLength)
            && Objects.equals(this.examples, that.examples)
            && Objects.equals(this.preferredToCategories, that.preferredToCategories)
            && Objects.equals(this.numMatches, that.numMatches)
            && Objects.equals(this.grokPattern, that.grokPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, categoryId, partitionFieldName, partitionFieldValue, terms, regex, maxMatchingLength, examples,
            preferredToCategories, numMatches, grokPattern);
    }
}
