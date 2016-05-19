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

package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder.CandidateGenerator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public final class DirectCandidateGeneratorBuilder implements CandidateGenerator {

    private static final String TYPE = "direct_generator";

    static final ParseField DIRECT_GENERATOR_FIELD = new ParseField(TYPE);
    static final ParseField FIELDNAME_FIELD = new ParseField("field");
    static final ParseField PREFILTER_FIELD = new ParseField("pre_filter");
    static final ParseField POSTFILTER_FIELD = new ParseField("post_filter");
    static final ParseField SUGGESTMODE_FIELD = new ParseField("suggest_mode");
    static final ParseField MIN_DOC_FREQ_FIELD = new ParseField("min_doc_freq");
    static final ParseField ACCURACY_FIELD = new ParseField("accuracy");
    static final ParseField SIZE_FIELD = new ParseField("size");
    static final ParseField SORT_FIELD = new ParseField("sort");
    static final ParseField STRING_DISTANCE_FIELD = new ParseField("string_distance");
    static final ParseField MAX_EDITS_FIELD = new ParseField("max_edits");
    static final ParseField MAX_INSPECTIONS_FIELD = new ParseField("max_inspections");
    static final ParseField MAX_TERM_FREQ_FIELD = new ParseField("max_term_freq");
    static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    static final ParseField MIN_WORD_LENGTH_FIELD = new ParseField("min_word_length");

    private final String field;
    private String preFilter;
    private String postFilter;
    private String suggestMode;
    private Float accuracy;
    private Integer size;
    private String sort;
    private String stringDistance;
    private Integer maxEdits;
    private Integer maxInspections;
    private Float maxTermFreq;
    private Integer prefixLength;
    private Integer minWordLength;
    private Float minDocFreq;

    /**
     * @param field Sets from what field to fetch the candidate suggestions from.
     */
    public DirectCandidateGeneratorBuilder(String field) {
        this.field = field;
    }

    /**
     * Quasi copy-constructor that takes all values from the generator
     * passed in, but uses different field name. Needed by parser because we
     * need to buffer the field name but read all other properties to a
     * temporary object.
     */
    private static DirectCandidateGeneratorBuilder replaceField(String field, DirectCandidateGeneratorBuilder other) {
        DirectCandidateGeneratorBuilder generator = new DirectCandidateGeneratorBuilder(field);
        generator.preFilter = other.preFilter;
        generator.postFilter = other.postFilter;
        generator.suggestMode = other.suggestMode;
        generator.accuracy = other.accuracy;
        generator.size = other.size;
        generator.sort = other.sort;
        generator.stringDistance = other.stringDistance;
        generator.maxEdits = other.maxEdits;
        generator.maxInspections = other.maxInspections;
        generator.maxTermFreq = other.maxTermFreq;
        generator.prefixLength = other.prefixLength;
        generator.minWordLength = other.minWordLength;
        generator.minDocFreq = other.minDocFreq;
        return generator;
    }

    /**
     * Read from a stream.
     */
    public DirectCandidateGeneratorBuilder(StreamInput in) throws IOException {
        field = in.readString();
        suggestMode = in.readOptionalString();
        accuracy = in.readOptionalFloat();
        size = in.readOptionalVInt();
        sort = in.readOptionalString();
        stringDistance = in.readOptionalString();
        maxEdits = in.readOptionalVInt();
        maxInspections = in.readOptionalVInt();
        maxTermFreq = in.readOptionalFloat();
        prefixLength = in.readOptionalVInt();
        minWordLength = in.readOptionalVInt();
        minDocFreq = in.readOptionalFloat();
        preFilter = in.readOptionalString();
        postFilter = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(suggestMode);
        out.writeOptionalFloat(accuracy);
        out.writeOptionalVInt(size);
        out.writeOptionalString(sort);
        out.writeOptionalString(stringDistance);
        out.writeOptionalVInt(maxEdits);
        out.writeOptionalVInt(maxInspections);
        out.writeOptionalFloat(maxTermFreq);
        out.writeOptionalVInt(prefixLength);
        out.writeOptionalVInt(minWordLength);
        out.writeOptionalFloat(minDocFreq);
        out.writeOptionalString(preFilter);
        out.writeOptionalString(postFilter);
    }

    /**
     * The global suggest mode controls what suggested terms are included or
     * controls for what suggest text tokens, terms should be suggested for.
     * Three possible values can be specified:
     * <ol>
     * <li><code>missing</code> - Only suggest terms in the suggest text
     * that aren't in the index. This is the default.
     * <li><code>popular</code> - Only suggest terms that occur in more docs
     * then the original suggest text term.
     * <li><code>always</code> - Suggest any matching suggest terms based on
     * tokens in the suggest text.
     * </ol>
     */
    public DirectCandidateGeneratorBuilder suggestMode(String suggestMode) {
        this.suggestMode = suggestMode;
        return this;
    }

    /**
     * Sets how similar the suggested terms at least need to be compared to
     * the original suggest text tokens. A value between 0 and 1 can be
     * specified. This value will be compared to the string distance result
     * of each candidate spelling correction.
     * <p>
     * Default is <tt>0.5</tt>
     */
    public DirectCandidateGeneratorBuilder accuracy(float accuracy) {
        this.accuracy = accuracy;
        return this;
    }

    /**
     * Sets the maximum suggestions to be returned per suggest text term.
     */
    public DirectCandidateGeneratorBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }
        this.size = size;
        return this;
    }

    /**
     * Sets how to sort the suggest terms per suggest text token. Two
     * possible values:
     * <ol>
     * <li><code>score</code> - Sort should first be based on score, then
     * document frequency and then the term itself.
     * <li><code>frequency</code> - Sort should first be based on document
     * frequency, then score and then the term itself.
     * </ol>
     * <p>
     * What the score is depends on the suggester being used.
     */
    public DirectCandidateGeneratorBuilder sort(String sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Sets what string distance implementation to use for comparing how
     * similar suggested terms are. Four possible values can be specified:
     * <ol>
     * <li><code>internal</code> - This is the default and is based on
     * <code>damerau_levenshtein</code>, but highly optimized for comparing
     * string distance for terms inside the index.
     * <li><code>damerau_levenshtein</code> - String distance algorithm
     * based on Damerau-Levenshtein algorithm.
     * <li><code>levenstein</code> - String distance algorithm based on
     * Levenstein edit distance algorithm.
     * <li><code>jarowinkler</code> - String distance algorithm based on
     * Jaro-Winkler algorithm.
     * <li><code>ngram</code> - String distance algorithm based on character
     * n-grams.
     * </ol>
     */
    public DirectCandidateGeneratorBuilder stringDistance(String stringDistance) {
        this.stringDistance = stringDistance;
        return this;
    }

    /**
     * Sets the maximum edit distance candidate suggestions can have in
     * order to be considered as a suggestion. Can only be a value between 1
     * and 2. Any other value result in an bad request error being thrown.
     * Defaults to <tt>2</tt>.
     */
    public DirectCandidateGeneratorBuilder maxEdits(Integer maxEdits) {
        if (maxEdits < 1 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
            throw new IllegalArgumentException("Illegal max_edits value " + maxEdits);
        }
        this.maxEdits = maxEdits;
        return this;
    }

    /**
     * A factor that is used to multiply with the size in order to inspect
     * more candidate suggestions. Can improve accuracy at the cost of
     * performance. Defaults to <tt>5</tt>.
     */
    public DirectCandidateGeneratorBuilder maxInspections(Integer maxInspections) {
        this.maxInspections = maxInspections;
        return this;
    }

    /**
     * Sets a maximum threshold in number of documents a suggest text token
     * can exist in order to be corrected. Can be a relative percentage
     * number (e.g 0.4) or an absolute number to represent document
     * frequencies. If an value higher than 1 is specified then fractional
     * can not be specified. Defaults to <tt>0.01</tt>.
     * <p>
     * This can be used to exclude high frequency terms from being
     * suggested. High frequency terms are usually spelled correctly on top
     * of this this also improves the suggest performance.
     */
    public DirectCandidateGeneratorBuilder maxTermFreq(float maxTermFreq) {
        this.maxTermFreq = maxTermFreq;
        return this;
    }

    /**
     * Sets the number of minimal prefix characters that must match in order
     * be a candidate suggestion. Defaults to 1. Increasing this number
     * improves suggest performance. Usually misspellings don't occur in the
     * beginning of terms.
     */
    public DirectCandidateGeneratorBuilder prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    /**
     * The minimum length a suggest text term must have in order to be
     * corrected. Defaults to <tt>4</tt>.
     */
    public DirectCandidateGeneratorBuilder minWordLength(int minWordLength) {
        this.minWordLength = minWordLength;
        return this;
    }

    /**
     * Sets a minimal threshold in number of documents a suggested term
     * should appear in. This can be specified as an absolute number or as a
     * relative percentage of number of documents. This can improve quality
     * by only suggesting high frequency terms. Defaults to 0f and is not
     * enabled. If a value higher than 1 is specified then the number cannot
     * be fractional.
     */
    public DirectCandidateGeneratorBuilder minDocFreq(float minDocFreq) {
        this.minDocFreq = minDocFreq;
        return this;
    }

    /**
     * Sets a filter (analyzer) that is applied to each of the tokens passed to this candidate generator.
     * This filter is applied to the original token before candidates are generated.
     */
    public DirectCandidateGeneratorBuilder preFilter(String preFilter) {
        this.preFilter = preFilter;
        return this;
    }

    /**
     * Sets a filter (analyzer) that is applied to each of the generated tokens
     * before they are passed to the actual phrase scorer.
     */
    public DirectCandidateGeneratorBuilder postFilter(String postFilter) {
        this.postFilter = postFilter;
        return this;
    }

    /**
     * gets the type identifier of this {@link CandidateGenerator}
     */
    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        outputFieldIfNotNull(field, FIELDNAME_FIELD, builder);
        outputFieldIfNotNull(accuracy, ACCURACY_FIELD, builder);
        outputFieldIfNotNull(maxEdits, MAX_EDITS_FIELD, builder);
        outputFieldIfNotNull(maxInspections, MAX_INSPECTIONS_FIELD, builder);
        outputFieldIfNotNull(maxTermFreq, MAX_TERM_FREQ_FIELD, builder);
        outputFieldIfNotNull(minWordLength, MIN_WORD_LENGTH_FIELD, builder);
        outputFieldIfNotNull(minDocFreq, MIN_DOC_FREQ_FIELD, builder);
        outputFieldIfNotNull(preFilter, PREFILTER_FIELD, builder);
        outputFieldIfNotNull(prefixLength, PREFIX_LENGTH_FIELD, builder);
        outputFieldIfNotNull(postFilter, POSTFILTER_FIELD, builder);
        outputFieldIfNotNull(suggestMode, SUGGESTMODE_FIELD, builder);
        outputFieldIfNotNull(size, SIZE_FIELD, builder);
        outputFieldIfNotNull(sort, SORT_FIELD, builder);
        outputFieldIfNotNull(stringDistance, STRING_DISTANCE_FIELD, builder);
        builder.endObject();
        return builder;
    }

    private static <T> void outputFieldIfNotNull(T value, ParseField field, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(field.getPreferredName(), value);
        }
    }

    private static ObjectParser<Tuple<Set<String>, DirectCandidateGeneratorBuilder>, QueryParseContext> PARSER = new ObjectParser<>(TYPE);

    static {
        PARSER.declareString((tp, s) -> tp.v1().add(s), FIELDNAME_FIELD);
        PARSER.declareString((tp, s) -> tp.v2().preFilter(s), PREFILTER_FIELD);
        PARSER.declareString((tp, s) -> tp.v2().postFilter(s), POSTFILTER_FIELD);
        PARSER.declareString((tp, s) -> tp.v2().suggestMode(s), SUGGESTMODE_FIELD);
        PARSER.declareFloat((tp, f) -> tp.v2().minDocFreq(f), MIN_DOC_FREQ_FIELD);
        PARSER.declareFloat((tp, f) -> tp.v2().accuracy(f), ACCURACY_FIELD);
        PARSER.declareInt((tp, i) -> tp.v2().size(i), SIZE_FIELD);
        PARSER.declareString((tp, s) -> tp.v2().sort(s), SORT_FIELD);
        PARSER.declareString((tp, s) -> tp.v2().stringDistance(s), STRING_DISTANCE_FIELD);
        PARSER.declareInt((tp, i) -> tp.v2().maxInspections(i), MAX_INSPECTIONS_FIELD);
        PARSER.declareFloat((tp, f) -> tp.v2().maxTermFreq(f), MAX_TERM_FREQ_FIELD);
        PARSER.declareInt((tp, i) -> tp.v2().maxEdits(i), MAX_EDITS_FIELD);
        PARSER.declareInt((tp, i) -> tp.v2().minWordLength(i), MIN_WORD_LENGTH_FIELD);
        PARSER.declareInt((tp, i) -> tp.v2().prefixLength(i), PREFIX_LENGTH_FIELD);
    }

    public static DirectCandidateGeneratorBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        DirectCandidateGeneratorBuilder tempGenerator = new DirectCandidateGeneratorBuilder("_na_");
        // bucket for the field name, needed as constructor arg later
        Set<String> tmpFieldName = new HashSet<>(1);
        PARSER.parse(parseContext.parser(), new Tuple<Set<String>, DirectCandidateGeneratorBuilder>(tmpFieldName, tempGenerator),
                parseContext);
        if (tmpFieldName.size() != 1) {
            throw new IllegalArgumentException("[" + TYPE + "] expects exactly one field parameter, but found " + tmpFieldName);
        }
        return replaceField(tmpFieldName.iterator().next(), tempGenerator);
    }

    @Override
    public PhraseSuggestionContext.DirectCandidateGenerator build(MapperService mapperService) throws IOException {
        PhraseSuggestionContext.DirectCandidateGenerator generator = new PhraseSuggestionContext.DirectCandidateGenerator();
        generator.setField(this.field);
        transferIfNotNull(this.size, generator::size);
        if (this.preFilter != null) {
            generator.preFilter(mapperService.analysisService().analyzer(this.preFilter));
            if (generator.preFilter() == null) {
                throw new IllegalArgumentException("Analyzer [" + this.preFilter + "] doesn't exists");
            }
        }
        if (this.postFilter != null) {
            generator.postFilter(mapperService.analysisService().analyzer(this.postFilter));
            if (generator.postFilter() == null) {
                throw new IllegalArgumentException("Analyzer [" + this.postFilter + "] doesn't exists");
            }
        }
        transferIfNotNull(this.accuracy, generator::accuracy);
        if (this.suggestMode != null) {
            generator.suggestMode(SuggestUtils.resolveSuggestMode(this.suggestMode));
        }
        if (this.sort != null) {
            generator.sort(SortBy.resolve(this.sort));
        }
        if (this.stringDistance != null) {
            generator.stringDistance(SuggestUtils.resolveDistance(this.stringDistance));
        }
        transferIfNotNull(this.maxEdits, generator::maxEdits);
        if (generator.maxEdits() < 1 || generator.maxEdits() > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
            throw new IllegalArgumentException("Illegal max_edits value " + generator.maxEdits());
        }
        transferIfNotNull(this.maxInspections, generator::maxInspections);
        transferIfNotNull(this.maxTermFreq, generator::maxTermFreq);
        transferIfNotNull(this.prefixLength, generator::prefixLength);
        transferIfNotNull(this.minWordLength, generator::minWordLength);
        transferIfNotNull(this.minDocFreq, generator::minDocFreq);
        return generator;
    }

     private static <T> void transferIfNotNull(T value, Consumer<T> consumer) {
         if (value != null) {
             consumer.accept(value);
         }
     }

    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }

    @Override
    public final int hashCode() {
        return Objects.hash(field, preFilter, postFilter, suggestMode, accuracy,
                size, sort, stringDistance, maxEdits, maxInspections,
                maxTermFreq, prefixLength, minWordLength, minDocFreq);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DirectCandidateGeneratorBuilder other = (DirectCandidateGeneratorBuilder) obj;
        return Objects.equals(field, other.field) &&
                Objects.equals(preFilter, other.preFilter) &&
                Objects.equals(postFilter, other.postFilter) &&
                Objects.equals(suggestMode, other.suggestMode) &&
                Objects.equals(accuracy, other.accuracy) &&
                Objects.equals(size, other.size) &&
                Objects.equals(sort, other.sort) &&
                Objects.equals(stringDistance, other.stringDistance) &&
                Objects.equals(maxEdits, other.maxEdits) &&
                Objects.equals(maxInspections, other.maxInspections) &&
                Objects.equals(maxTermFreq, other.maxTermFreq) &&
                Objects.equals(prefixLength, other.prefixLength) &&
                Objects.equals(minWordLength, other.minWordLength) &&
                Objects.equals(minDocFreq, other.minDocFreq);
    }
}
