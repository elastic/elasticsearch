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
import java.util.Arrays;
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

    public static final ConstructingObjectParser<CategoryDefinition, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<CategoryDefinition, Void> LENIENT_PARSER = createParser(true);

    private static ConstructingObjectParser<CategoryDefinition, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<CategoryDefinition, Void> parser = new ConstructingObjectParser<>(TYPE.getPreferredName(),
                ignoreUnknownFields, a -> new CategoryDefinition((String) a[0]));

        parser.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        parser.declareLong(CategoryDefinition::setCategoryId, CATEGORY_ID);
        parser.declareString(CategoryDefinition::setPartitionFieldName, PARTITION_FIELD_NAME);
        parser.declareString(CategoryDefinition::setPartitionFieldValue, PARTITION_FIELD_VALUE);
        parser.declareString(CategoryDefinition::setTerms, TERMS);
        parser.declareString(CategoryDefinition::setRegex, REGEX);
        parser.declareLong(CategoryDefinition::setMaxMatchingLength, MAX_MATCHING_LENGTH);
        parser.declareStringArray(CategoryDefinition::setExamples, EXAMPLES);
        parser.declareString(CategoryDefinition::setGrokPattern, GROK_PATTERN);
        parser.declareLongArray(CategoryDefinition::setPreferredToCategories, PREFERRED_TO_CATEGORIES);
        parser.declareLong(CategoryDefinition::setNumMatches, NUM_MATCHES);
        return parser;
    }

    private final String jobId;
    private long categoryId = 0L;
    private String partitionFieldName;
    private String partitionFieldValue;
    private String terms = "";
    private String regex = "";
    private long maxMatchingLength = 0L;
    private final Set<String> examples;
    private String grokPattern;
    private long[] preferredToCategories = new long[0];
    private long numMatches = 0L;

    public CategoryDefinition(String jobId) {
        this.jobId = jobId;
        examples = new TreeSet<>();
    }

    public CategoryDefinition(StreamInput in) throws IOException {
        jobId = in.readString();
        categoryId = in.readLong();
        partitionFieldName = in.readOptionalString();
        partitionFieldValue = in.readOptionalString();
        terms = in.readString();
        regex = in.readString();
        maxMatchingLength = in.readLong();
        examples = new TreeSet<>(in.readStringList());
        grokPattern = in.readOptionalString();
        this.preferredToCategories = in.readVLongArray();
        this.numMatches = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(categoryId);
        out.writeOptionalString(partitionFieldName);
        out.writeOptionalString(partitionFieldValue);
        out.writeString(terms);
        out.writeString(regex);
        out.writeLong(maxMatchingLength);
        out.writeStringCollection(examples);
        out.writeOptionalString(grokPattern);
        out.writeVLongArray(preferredToCategories);
        out.writeVLong(numMatches);
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

    public long[] getPreferredToCategories() {
        return preferredToCategories;
    }

    public void setPreferredToCategories(long[] preferredToCategories) {
        for (long category : preferredToCategories) {
            if (category < 0) {
                throw new IllegalArgumentException("[preferred_to_category] entries must be non-negative");
            }
        }
        this.preferredToCategories = preferredToCategories;
    }

    private void setPreferredToCategories(List<Long> preferredToCategories) {
        setPreferredToCategories(preferredToCategories.stream().mapToLong(Long::longValue).toArray());
    }

    public long getNumMatches() {
        return numMatches;
    }

    public void setNumMatches(long numMatches) {
        if (numMatches < 0) {
            throw new IllegalArgumentException("[num_matches] must be non-negative");
        }
        this.numMatches = numMatches;
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
        if (preferredToCategories.length > 0) {
            builder.field(PREFERRED_TO_CATEGORIES.getPreferredName(), preferredToCategories);
        }
        if (numMatches > 0) {
            builder.field(NUM_MATCHES.getPreferredName(), numMatches);
        }

        // Copy the patten from AnomalyRecord that by/over/partition field values are added to results
        // as key value pairs after all the fixed fields if they won't clash with reserved fields
        if (partitionFieldName != null && partitionFieldValue != null && ReservedFieldNames.isValidFieldName(partitionFieldName)) {
            builder.field(partitionFieldName, partitionFieldValue);
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
                && Objects.equals(this.partitionFieldName, that.partitionFieldName)
                && Objects.equals(this.partitionFieldValue, that.partitionFieldValue)
                && Objects.equals(this.terms, that.terms)
                && Objects.equals(this.regex, that.regex)
                && Objects.equals(this.maxMatchingLength, that.maxMatchingLength)
                && Objects.equals(this.examples, that.examples)
                && Objects.equals(this.grokPattern, that.grokPattern)
                && Arrays.equals(this.preferredToCategories, that.preferredToCategories)
                && Objects.equals(this.numMatches, that.numMatches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId,
            categoryId,
            partitionFieldName,
            partitionFieldValue,
            terms,
            regex,
            maxMatchingLength,
            examples,
            grokPattern,
            Arrays.hashCode(preferredToCategories),
            numMatches);
    }
}
