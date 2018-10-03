/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class CategoryDefinition implements ToXContentObject {

    public static final ParseField TYPE = new ParseField("category_definition");

    public static final ParseField CATEGORY_ID = new ParseField("category_id");
    public static final ParseField TERMS = new ParseField("terms");
    public static final ParseField REGEX = new ParseField("regex");
    public static final ParseField MAX_MATCHING_LENGTH = new ParseField("max_matching_length");
    public static final ParseField EXAMPLES = new ParseField("examples");
    public static final ParseField GROK_PATTERN = new ParseField("grok_pattern");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("categories");

    public static final ConstructingObjectParser<CategoryDefinition, Void> PARSER =
        new ConstructingObjectParser<>(TYPE.getPreferredName(), true, a -> new CategoryDefinition((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareLong(CategoryDefinition::setCategoryId, CATEGORY_ID);
        PARSER.declareString(CategoryDefinition::setTerms, TERMS);
        PARSER.declareString(CategoryDefinition::setRegex, REGEX);
        PARSER.declareLong(CategoryDefinition::setMaxMatchingLength, MAX_MATCHING_LENGTH);
        PARSER.declareStringArray(CategoryDefinition::setExamples, EXAMPLES);
        PARSER.declareString(CategoryDefinition::setGrokPattern, GROK_PATTERN);
    }

    private final String jobId;
    private long categoryId = 0L;
    private String terms = "";
    private String regex = "";
    private long maxMatchingLength = 0L;
    private final Set<String> examples = new TreeSet<>();
    private String grokPattern;

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
        if (other == null || getClass() != other.getClass()) {
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
