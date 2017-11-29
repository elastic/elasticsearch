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


import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionContext.DirectCandidateGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * Defines the actual suggest command for phrase suggestions ( <tt>phrase</tt>).
 */
public class PhraseSuggestionBuilder extends SuggestionBuilder<PhraseSuggestionBuilder> {

    private static final String SUGGESTION_NAME = "phrase";

    protected static final ParseField MAXERRORS_FIELD = new ParseField("max_errors");
    protected static final ParseField RWE_LIKELIHOOD_FIELD = new ParseField("real_word_error_likelihood");
    protected static final ParseField SEPARATOR_FIELD = new ParseField("separator");
    protected static final ParseField CONFIDENCE_FIELD = new ParseField("confidence");
    protected static final ParseField GRAMSIZE_FIELD = new ParseField("gram_size");
    protected static final ParseField SMOOTHING_MODEL_FIELD = new ParseField("smoothing");
    protected static final ParseField FORCE_UNIGRAM_FIELD = new ParseField("force_unigrams");
    protected static final ParseField TOKEN_LIMIT_FIELD = new ParseField("token_limit");
    protected static final ParseField HIGHLIGHT_FIELD = new ParseField("highlight");
    protected static final ParseField PRE_TAG_FIELD = new ParseField("pre_tag");
    protected static final ParseField POST_TAG_FIELD = new ParseField("post_tag");
    protected static final ParseField COLLATE_FIELD = new ParseField("collate");
    protected static final ParseField COLLATE_QUERY_FIELD = new ParseField("query");
    protected static final ParseField COLLATE_QUERY_PARAMS = new ParseField("params");
    protected static final ParseField COLLATE_QUERY_PRUNE = new ParseField("prune");

    private float maxErrors = PhraseSuggestionContext.DEFAULT_MAX_ERRORS;
    private String separator = PhraseSuggestionContext.DEFAULT_SEPARATOR;
    private float realWordErrorLikelihood = PhraseSuggestionContext.DEFAULT_RWE_ERRORLIKELIHOOD;
    private float confidence = PhraseSuggestionContext.DEFAULT_CONFIDENCE;
    // gramSize needs to be optional although there is a default, if unset parser try to detect and use shingle size
    private Integer gramSize;
    private boolean forceUnigrams = PhraseSuggestionContext.DEFAULT_REQUIRE_UNIGRAM;
    private int tokenLimit = NoisyChannelSpellChecker.DEFAULT_TOKEN_LIMIT;
    private String preTag;
    private String postTag;
    private Script collateQuery;
    private Map<String, Object> collateParams;
    private boolean collatePrune = PhraseSuggestionContext.DEFAULT_COLLATE_PRUNE;
    private SmoothingModel model;
    private final Map<String, List<CandidateGenerator>> generators = new HashMap<>();

    public PhraseSuggestionBuilder(String field) {
        super(field);
    }

    /**
     * internal copy constructor that copies over all class fields except for the field which is
     * set to the one provided in the first argument
     */
    private PhraseSuggestionBuilder(String fieldname, PhraseSuggestionBuilder in) {
        super(fieldname, in);
        maxErrors = in.maxErrors;
        separator = in.separator;
        realWordErrorLikelihood = in.realWordErrorLikelihood;
        confidence = in.confidence;
        gramSize = in.gramSize;
        forceUnigrams = in.forceUnigrams;
        tokenLimit = in.tokenLimit;
        preTag = in.preTag;
        postTag = in.postTag;
        collateQuery = in.collateQuery;
        collateParams = in.collateParams;
        collatePrune = in.collatePrune;
        model = in.model;
        generators.putAll(in.generators);
    }

    /**
     * Read from a stream.
     */
    public PhraseSuggestionBuilder(StreamInput in) throws IOException {
        super(in);
        maxErrors = in.readFloat();
        realWordErrorLikelihood = in.readFloat();
        confidence = in.readFloat();
        gramSize = in.readOptionalVInt();
        model = in.readOptionalNamedWriteable(SmoothingModel.class);
        forceUnigrams = in.readBoolean();
        tokenLimit = in.readVInt();
        preTag = in.readOptionalString();
        postTag = in.readOptionalString();
        separator = in.readString();
        if (in.readBoolean()) {
            collateQuery = new Script(in);
        }
        collateParams = in.readMap();
        collatePrune = in.readOptionalBoolean();
        int generatorsEntries = in.readVInt();
        for (int i = 0; i < generatorsEntries; i++) {
            String type = in.readString();
            int numberOfGenerators = in.readVInt();
            List<CandidateGenerator> generatorsList = new ArrayList<>(numberOfGenerators);
            for (int g = 0; g < numberOfGenerators; g++) {
                DirectCandidateGeneratorBuilder generator = new DirectCandidateGeneratorBuilder(in);
                generatorsList.add(generator);
            }
            generators.put(type, generatorsList);
        }
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeFloat(maxErrors);
        out.writeFloat(realWordErrorLikelihood);
        out.writeFloat(confidence);
        out.writeOptionalVInt(gramSize);
        out.writeOptionalNamedWriteable(model);
        out.writeBoolean(forceUnigrams);
        out.writeVInt(tokenLimit);
        out.writeOptionalString(preTag);
        out.writeOptionalString(postTag);
        out.writeString(separator);
        if (collateQuery != null) {
            out.writeBoolean(true);
            collateQuery.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeMapWithConsistentOrder(collateParams);
        out.writeOptionalBoolean(collatePrune);
        out.writeVInt(this.generators.size());
        for (Entry<String, List<CandidateGenerator>> entry : this.generators.entrySet()) {
            out.writeString(entry.getKey());
            List<CandidateGenerator> generatorsList = entry.getValue();
            out.writeVInt(generatorsList.size());
            for (CandidateGenerator generator : generatorsList) {
                generator.writeTo(out);
            }
        }
    }

    /**
     * Sets the gram size for the n-gram model used for this suggester. The
     * default value is <tt>1</tt> corresponding to <tt>unigrams</tt>. Use
     * <tt>2</tt> for <tt>bigrams</tt> and <tt>3</tt> for <tt>trigrams</tt>.
     */
    public PhraseSuggestionBuilder gramSize(int gramSize) {
        if (gramSize < 1) {
            throw new IllegalArgumentException("gramSize must be >= 1");
        }
        this.gramSize = gramSize;
        return this;
    }

    /**
     * get the {@link #gramSize(int)} parameter
     */
    public Integer gramSize() {
        return this.gramSize;
    }

    /**
     * Sets the maximum percentage of the terms that at most considered to be
     * misspellings in order to form a correction. This method accepts a float
     * value in the range [0..1) as a fraction of the actual query terms a
     * number <tt>&gt;=1</tt> as an absolute number of query terms.
     *
     * The default is set to <tt>1.0</tt> which corresponds to that only
     * corrections with at most 1 misspelled term are returned.
     */
    public PhraseSuggestionBuilder maxErrors(float maxErrors) {
        if (maxErrors <= 0.0) {
            throw new IllegalArgumentException("max_error must be > 0.0");
        }
        this.maxErrors = maxErrors;
        return this;
    }

    /**
     * get the maxErrors setting
     */
    public Float maxErrors() {
        return this.maxErrors;
    }

    /**
     * Sets the separator that is used to separate terms in the bigram field. If
     * not set the whitespace character is used as a separator.
     */
    public PhraseSuggestionBuilder separator(String separator) {
        Objects.requireNonNull(separator, "separator cannot be set to null");
        this.separator = separator;
        return this;
    }

    /**
     * get the separator that is used to separate terms in the bigram field.
     */
    public String separator() {
        return this.separator;
    }

    /**
     * Sets the likelihood of a term being a misspelled even if the term exists
     * in the dictionary. The default it <tt>0.95</tt> corresponding to 5% or
     * the real words are misspelled.
     */
    public PhraseSuggestionBuilder realWordErrorLikelihood(float realWordErrorLikelihood) {
        if (realWordErrorLikelihood <= 0.0) {
            throw new IllegalArgumentException("real_word_error_likelihood must be > 0.0");
        }
        this.realWordErrorLikelihood = realWordErrorLikelihood;
        return this;
    }

    /**
     * get the {@link #realWordErrorLikelihood(float)} parameter
     */
    public Float realWordErrorLikelihood() {
        return this.realWordErrorLikelihood;
    }

    /**
     * Sets the confidence level for this suggester. The confidence level
     * defines a factor applied to the input phrases score which is used as a
     * threshold for other suggest candidates. Only candidates that score higher
     * than the threshold will be included in the result. For instance a
     * confidence level of <tt>1.0</tt> will only return suggestions that score
     * higher than the input phrase. If set to <tt>0.0</tt> the top N candidates
     * are returned. The default is <tt>1.0</tt>
     */
    public PhraseSuggestionBuilder confidence(float confidence) {
        if (confidence < 0.0) {
            throw new IllegalArgumentException("confidence must be >= 0.0");
        }
        this.confidence = confidence;
        return this;
    }

    /**
     * get the {@link #confidence()} parameter
     */
    public Float confidence() {
        return this.confidence;
    }

    /**
     * Adds a {@link CandidateGenerator} to this suggester. The
     * {@link CandidateGenerator} is used to draw candidates for each individual
     * phrase term before the candidates are scored.
     */
    public PhraseSuggestionBuilder addCandidateGenerator(CandidateGenerator generator) {
        List<CandidateGenerator> list = this.generators.get(generator.getType());
        if (list == null) {
            list = new ArrayList<>();
            this.generators.put(generator.getType(), list);
        }
        list.add(generator);
        return this;
    }

    /**
     * Clear the candidate generators.
     */
    public PhraseSuggestionBuilder clearCandidateGenerators() {
        this.generators.clear();
        return this;
    }

    /**
     * get the candidate generators.
     */
    Map<String, List<CandidateGenerator>> getCandidateGenerators() {
        return this.generators;
    }

    /**
     * If set to <code>true</code> the phrase suggester will fail if the analyzer only
     * produces ngrams. the default it <code>true</code>.
     */
    public PhraseSuggestionBuilder forceUnigrams(boolean forceUnigrams) {
        this.forceUnigrams = forceUnigrams;
        return this;
    }

    /**
     * get the setting for {@link #forceUnigrams()}
     */
    public Boolean forceUnigrams() {
        return this.forceUnigrams;
    }

    /**
     * Sets an explicit smoothing model used for this suggester. The default is
     * {@link StupidBackoff}.
     */
    public PhraseSuggestionBuilder smoothingModel(SmoothingModel model) {
        this.model = model;
        return this;
    }

    /**
     * Gets the {@link SmoothingModel}
     */
    public SmoothingModel smoothingModel() {
        return this.model;
    }

    public PhraseSuggestionBuilder tokenLimit(int tokenLimit) {
        if (tokenLimit <= 0) {
            throw new IllegalArgumentException("token_limit must be >= 1");
        }
        this.tokenLimit = tokenLimit;
        return this;
    }

    /**
     * get the {@link #tokenLimit(int)} parameter
     */
    public Integer tokenLimit() {
        return this.tokenLimit;
    }

    /**
     * Setup highlighting for suggestions.  If this is called a highlight field
     * is returned with suggestions wrapping changed tokens with preTag and postTag.
     */
    public PhraseSuggestionBuilder highlight(String preTag, String postTag) {
        if ((preTag == null) != (postTag == null)) {
            throw new IllegalArgumentException("Pre and post tag must both be null or both not be null.");
        }
        this.preTag = preTag;
        this.postTag = postTag;
        return this;
    }

    /**
     * get the pre-tag for the highlighter set with {@link #highlight(String, String)}
     */
    public String preTag() {
        return this.preTag;
    }

    /**
     * get the post-tag for the highlighter set with {@link #highlight(String, String)}
     */
    public String postTag() {
        return this.postTag;
    }

    /**
     * Sets a query used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateQuery(String collateQuery) {
        this.collateQuery = new Script(ScriptType.INLINE, "mustache", collateQuery, Collections.emptyMap());
        return this;
    }

    /**
     * Sets a query used for filtering out suggested phrases (collation).
     */
    public PhraseSuggestionBuilder collateQuery(Script collateQueryTemplate) {
        this.collateQuery = collateQueryTemplate;
        return this;
    }

    /**
     * gets the query used for filtering out suggested phrases (collation).
     */
    public Script collateQuery() {
        return this.collateQuery;
    }

    /**
     * Adds additional parameters for collate scripts. Previously added parameters on the
     * same builder will be overwritten.
     */
    public PhraseSuggestionBuilder collateParams(Map<String, Object> collateParams) {
        Objects.requireNonNull(collateParams, "collate parameters cannot be null.");
        this.collateParams = new HashMap<>(collateParams);
        return this;
    }

    /**
     * gets additional params for collate script
     */
    public Map<String, Object> collateParams() {
        return this.collateParams;
    }

    /**
     * Sets whether to prune suggestions after collation
     */
    public PhraseSuggestionBuilder collatePrune(boolean collatePrune) {
        this.collatePrune = collatePrune;
        return this;
    }

    /**
     * Gets whether to prune suggestions after collation
     */
    public Boolean collatePrune() {
        return this.collatePrune;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RWE_LIKELIHOOD_FIELD.getPreferredName(), realWordErrorLikelihood);
        builder.field(CONFIDENCE_FIELD.getPreferredName(), confidence);
        builder.field(SEPARATOR_FIELD.getPreferredName(), separator);
        builder.field(MAXERRORS_FIELD.getPreferredName(), maxErrors);
        if (gramSize != null) {
            builder.field(GRAMSIZE_FIELD.getPreferredName(), gramSize);
        }
        builder.field(FORCE_UNIGRAM_FIELD.getPreferredName(), forceUnigrams);
        builder.field(TOKEN_LIMIT_FIELD.getPreferredName(), tokenLimit);
        if (!generators.isEmpty()) {
            Set<Entry<String, List<CandidateGenerator>>> entrySet = generators.entrySet();
            for (Entry<String, List<CandidateGenerator>> entry : entrySet) {
                builder.startArray(entry.getKey());
                for (CandidateGenerator generator : entry.getValue()) {
                    generator.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        if (model != null) {
            builder.startObject(SMOOTHING_MODEL_FIELD.getPreferredName());
            model.toXContent(builder, params);
            builder.endObject();
        }
        if (preTag != null) {
            builder.startObject(HIGHLIGHT_FIELD.getPreferredName());
            builder.field(PRE_TAG_FIELD.getPreferredName(), preTag);
            builder.field(POST_TAG_FIELD.getPreferredName(), postTag);
            builder.endObject();
        }
        if (collateQuery != null) {
            builder.startObject(COLLATE_FIELD.getPreferredName());
            builder.field(COLLATE_QUERY_FIELD.getPreferredName(), collateQuery);
            if (collateParams != null) {
                builder.field(COLLATE_QUERY_PARAMS.getPreferredName(), collateParams);
            }
            builder.field(COLLATE_QUERY_PRUNE.getPreferredName(), collatePrune);
            builder.endObject();
        }
        return builder;
    }

    public static PhraseSuggestionBuilder fromXContent(XContentParser parser) throws IOException {
        PhraseSuggestionBuilder tmpSuggestion = new PhraseSuggestionBuilder("_na_");
        XContentParser.Token token;
        String currentFieldName = null;
        String fieldname = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SuggestionBuilder.ANALYZER_FIELD.match(currentFieldName)) {
                    tmpSuggestion.analyzer(parser.text());
                } else if (SuggestionBuilder.FIELDNAME_FIELD.match(currentFieldName)) {
                    fieldname = parser.text();
                } else if (SuggestionBuilder.SIZE_FIELD.match(currentFieldName)) {
                    tmpSuggestion.size(parser.intValue());
                } else if (SuggestionBuilder.SHARDSIZE_FIELD.match(currentFieldName)) {
                    tmpSuggestion.shardSize(parser.intValue());
                } else if (PhraseSuggestionBuilder.RWE_LIKELIHOOD_FIELD.match(currentFieldName)) {
                    tmpSuggestion.realWordErrorLikelihood(parser.floatValue());
                } else if (PhraseSuggestionBuilder.CONFIDENCE_FIELD.match(currentFieldName)) {
                    tmpSuggestion.confidence(parser.floatValue());
                } else if (PhraseSuggestionBuilder.SEPARATOR_FIELD.match(currentFieldName)) {
                    tmpSuggestion.separator(parser.text());
                } else if (PhraseSuggestionBuilder.MAXERRORS_FIELD.match(currentFieldName)) {
                    tmpSuggestion.maxErrors(parser.floatValue());
                } else if (PhraseSuggestionBuilder.GRAMSIZE_FIELD.match(currentFieldName)) {
                    tmpSuggestion.gramSize(parser.intValue());
                } else if (PhraseSuggestionBuilder.FORCE_UNIGRAM_FIELD.match(currentFieldName)) {
                    tmpSuggestion.forceUnigrams(parser.booleanValue());
                } else if (PhraseSuggestionBuilder.TOKEN_LIMIT_FIELD.match(currentFieldName)) {
                    tmpSuggestion.tokenLimit(parser.intValue());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "suggester[phrase] doesn't support field [" + currentFieldName + "]");
                }
            } else if (token == Token.START_ARRAY) {
                if (DirectCandidateGeneratorBuilder.DIRECT_GENERATOR_FIELD.match(currentFieldName)) {
                    // for now we only have a single type of generators
                    while ((token = parser.nextToken()) == Token.START_OBJECT) {
                        tmpSuggestion.addCandidateGenerator(DirectCandidateGeneratorBuilder.PARSER.apply(parser, null));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "suggester[phrase]  doesn't support array field [" + currentFieldName + "]");
                }
            } else if (token == Token.START_OBJECT) {
                if (PhraseSuggestionBuilder.SMOOTHING_MODEL_FIELD.match(currentFieldName)) {
                    ensureNoSmoothing(tmpSuggestion);
                    tmpSuggestion.smoothingModel(SmoothingModel.fromXContent(parser));
                } else if (PhraseSuggestionBuilder.HIGHLIGHT_FIELD.match(currentFieldName)) {
                    String preTag = null;
                    String postTag = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (PhraseSuggestionBuilder.PRE_TAG_FIELD.match(currentFieldName)) {
                                preTag = parser.text();
                            } else if (PhraseSuggestionBuilder.POST_TAG_FIELD.match(currentFieldName)) {
                                postTag = parser.text();
                            } else {
                                throw new ParsingException(parser.getTokenLocation(),
                                    "suggester[phrase][highlight] doesn't support field [" + currentFieldName + "]");
                            }
                        }
                    }
                    tmpSuggestion.highlight(preTag, postTag);
                } else if (PhraseSuggestionBuilder.COLLATE_FIELD.match(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (PhraseSuggestionBuilder.COLLATE_QUERY_FIELD.match(currentFieldName)) {
                            if (tmpSuggestion.collateQuery() != null) {
                                throw new ParsingException(parser.getTokenLocation(),
                                        "suggester[phrase][collate] query already set, doesn't support additional ["
                                        + currentFieldName + "]");
                            }
                            Script template = Script.parse(parser, Script.DEFAULT_TEMPLATE_LANG);
                            tmpSuggestion.collateQuery(template);
                        } else if (PhraseSuggestionBuilder.COLLATE_QUERY_PARAMS.match(currentFieldName)) {
                            tmpSuggestion.collateParams(parser.map());
                        } else if (PhraseSuggestionBuilder.COLLATE_QUERY_PRUNE.match(currentFieldName)) {
                            if (parser.isBooleanValue()) {
                                tmpSuggestion.collatePrune(parser.booleanValue());
                            } else {
                                throw new ParsingException(parser.getTokenLocation(),
                                        "suggester[phrase][collate] prune must be either 'true' or 'false'");
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "suggester[phrase][collate] doesn't support field [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "suggester[phrase]  doesn't support array field [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "suggester[phrase] doesn't support field [" + currentFieldName + "]");
            }
        }

        // now we should have field name, check and copy fields over to the suggestion builder we return
        if (fieldname == null) {
            throw new ElasticsearchParseException(
                "the required field option [" + FIELDNAME_FIELD.getPreferredName() + "] is missing");
        }
        return new PhraseSuggestionBuilder(fieldname, tmpSuggestion);
    }


    @Override
    public SuggestionContext build(QueryShardContext context) throws IOException {
        PhraseSuggestionContext suggestionContext = new PhraseSuggestionContext(context);
        MapperService mapperService = context.getMapperService();
        // copy over common settings to each suggestion builder
        populateCommonFields(mapperService, suggestionContext);

        suggestionContext.setSeparator(BytesRefs.toBytesRef(this.separator));
        suggestionContext.setRealWordErrorLikelihood(this.realWordErrorLikelihood);
        suggestionContext.setConfidence(this.confidence);
        suggestionContext.setMaxErrors(this.maxErrors);
        suggestionContext.setRequireUnigram(this.forceUnigrams);
        suggestionContext.setTokenLimit(this.tokenLimit);
        suggestionContext.setPreTag(BytesRefs.toBytesRef(this.preTag));
        suggestionContext.setPostTag(BytesRefs.toBytesRef(this.postTag));

        if (this.gramSize != null) {
            suggestionContext.setGramSize(this.gramSize);
        }

        for (List<CandidateGenerator> candidateGenerators : this.generators.values()) {
            for (CandidateGenerator candidateGenerator : candidateGenerators) {
                suggestionContext.addGenerator(candidateGenerator.build(mapperService));
            }
        }

        if (this.model != null) {
            suggestionContext.setModel(this.model.buildWordScorerFactory());
        }

        if (this.collateQuery != null) {
            TemplateScript.Factory scriptFactory = context.getScriptService().compile(this.collateQuery, TemplateScript.CONTEXT);
            suggestionContext.setCollateQueryScript(scriptFactory);
            if (this.collateParams != null) {
                suggestionContext.setCollateScriptParams(this.collateParams);
            }
            suggestionContext.setCollatePrune(this.collatePrune);
        }

        if (this.gramSize == null || suggestionContext.generators().isEmpty()) {
            final ShingleTokenFilterFactory.Factory shingleFilterFactory = getShingleFilterFactory(suggestionContext.getAnalyzer());
            if (this.gramSize == null) {
                // try to detect the shingle size
                if (shingleFilterFactory != null) {
                    suggestionContext.setGramSize(shingleFilterFactory.getMaxShingleSize());
                    if (suggestionContext.getAnalyzer() == null && shingleFilterFactory.getMinShingleSize() > 1
                            && !shingleFilterFactory.getOutputUnigrams()) {
                        throw new IllegalArgumentException("The default analyzer for field: [" + suggestionContext.getField()
                                + "] doesn't emit unigrams. If this is intentional try to set the analyzer explicitly");
                    }
                }
            }
            if (suggestionContext.generators().isEmpty()) {
                if (shingleFilterFactory != null && shingleFilterFactory.getMinShingleSize() > 1
                        && !shingleFilterFactory.getOutputUnigrams() && suggestionContext.getRequireUnigram()) {
                    throw new IllegalArgumentException("The default candidate generator for phrase suggest can't operate on field: ["
                            + suggestionContext.getField() + "] since it doesn't emit unigrams. "
                            + "If this is intentional try to set the candidate generator field explicitly");
                }
                // use a default generator on the same field
                DirectCandidateGenerator generator = new DirectCandidateGenerator();
                generator.setField(suggestionContext.getField());
                suggestionContext.addGenerator(generator);
            }
        }
        return suggestionContext;
    }

    private static ShingleTokenFilterFactory.Factory getShingleFilterFactory(Analyzer analyzer) {
        if (analyzer instanceof NamedAnalyzer) {
            analyzer = ((NamedAnalyzer)analyzer).analyzer();
        }
        if (analyzer instanceof CustomAnalyzer) {
            final CustomAnalyzer a = (CustomAnalyzer) analyzer;
            final TokenFilterFactory[] tokenFilters = a.tokenFilters();
            for (TokenFilterFactory tokenFilterFactory : tokenFilters) {
                if (tokenFilterFactory instanceof ShingleTokenFilterFactory) {
                    return ((ShingleTokenFilterFactory)tokenFilterFactory).getInnerFactory();
                } else if (tokenFilterFactory instanceof ShingleTokenFilterFactory.Factory) {
                    return (ShingleTokenFilterFactory.Factory) tokenFilterFactory;
                }
            }
        }
        return null;
    }

    private static void ensureNoSmoothing(PhraseSuggestionBuilder suggestion) {
        if (suggestion.smoothingModel() != null) {
            throw new IllegalArgumentException("only one smoothing model supported");
        }
    }

    @Override
    public String getWriteableName() {
        return SUGGESTION_NAME;
    }

    @Override
    protected boolean doEquals(PhraseSuggestionBuilder other) {
        return Objects.equals(maxErrors, other.maxErrors) &&
                Objects.equals(separator, other.separator) &&
                Objects.equals(realWordErrorLikelihood, other.realWordErrorLikelihood) &&
                Objects.equals(confidence, other.confidence) &&
                Objects.equals(generators, other.generators) &&
                Objects.equals(gramSize, other.gramSize) &&
                Objects.equals(model, other.model) &&
                Objects.equals(forceUnigrams, other.forceUnigrams) &&
                Objects.equals(tokenLimit, other.tokenLimit) &&
                Objects.equals(preTag, other.preTag) &&
                Objects.equals(postTag, other.postTag) &&
                Objects.equals(collateQuery, other.collateQuery) &&
                Objects.equals(collateParams, other.collateParams) &&
                Objects.equals(collatePrune, other.collatePrune);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(maxErrors, separator, realWordErrorLikelihood, confidence,
                generators, gramSize, model, forceUnigrams, tokenLimit, preTag, postTag,
                collateQuery, collateParams, collatePrune);
    }

    /**
     * {@link CandidateGenerator} interface.
     */
    public interface CandidateGenerator extends Writeable, ToXContentObject {
        String getType();

        PhraseSuggestionContext.DirectCandidateGenerator build(MapperService mapperService) throws IOException;
    }
}
