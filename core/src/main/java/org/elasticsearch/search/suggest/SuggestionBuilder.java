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

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for the different suggestion implementations.
 */
public abstract class SuggestionBuilder<T extends SuggestionBuilder<T>> extends ToXContentToBytes implements NamedWriteable<T> {

    protected final String name;
    // TODO this seems mandatory and should be constructor arg
    protected String fieldname;
    protected String text;
    protected String prefix;
    protected String regex;
    protected String analyzer;
    protected Integer size;
    protected Integer shardSize;

    protected static final ParseField TEXT_FIELD = new ParseField("text");
    protected static final ParseField PREFIX_FIELD = new ParseField("prefix");
    protected static final ParseField REGEX_FIELD = new ParseField("regex");
    protected static final ParseField FIELDNAME_FIELD = new ParseField("field");
    protected static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    protected static final ParseField SIZE_FIELD = new ParseField("size");
    protected static final ParseField SHARDSIZE_FIELD = new ParseField("shard_size");

    public SuggestionBuilder(String name) {
        Objects.requireNonNull(name, "Suggester 'name' cannot be null");
        this.name = name;
    }

    /**
     * get the name for this suggestion
     */
    public String name() {
        return this.name;
    }

    /**
     * Same as in {@link SuggestBuilder#setText(String)}, but in the suggestion scope.
     */
    @SuppressWarnings("unchecked")
    public T text(String text) {
        this.text = text;
        return (T) this;
    }

    /**
     * get the text for this suggestion
     */
    public String text() {
        return this.text;
    }

    @SuppressWarnings("unchecked")
    protected T prefix(String prefix) {
        this.prefix = prefix;
        return (T) this;
    }

    /**
     * get the prefix for this suggestion
     */
    public String prefix() {
        return this.prefix;
    }

    @SuppressWarnings("unchecked")
    protected T regex(String regex) {
        this.regex = regex;
        return (T) this;
    }

    /**
     * get the regex for this suggestion
     */
    public String regex() {
        return this.regex;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        if (text != null) {
            builder.field(TEXT_FIELD.getPreferredName(), text);
        }
        if (prefix != null) {
            builder.field(PREFIX_FIELD.getPreferredName(), prefix);
        }
        if (regex != null) {
            builder.field(REGEX_FIELD.getPreferredName(), regex);
        }
        builder.startObject(getSuggesterName());
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        if (fieldname != null) {
            builder.field(FIELDNAME_FIELD.getPreferredName(), fieldname);
        }
        if (size != null) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }
        if (shardSize != null) {
            builder.field(SHARDSIZE_FIELD.getPreferredName(), shardSize);
        }

        builder = innerToXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    public static SuggestionBuilder<?> fromXContent(QueryParseContext parseContext, String suggestionName, Suggesters suggesters)
            throws IOException {
        XContentParser parser = parseContext.parser();
        ParseFieldMatcher parsefieldMatcher = parseContext.parseFieldMatcher();
        XContentParser.Token token;
        String fieldName = null;
        String suggestText = null;
        String prefix = null;
        String regex = null;
        SuggestionBuilder<?> suggestionBuilder = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parsefieldMatcher.match(fieldName, TEXT_FIELD)) {
                    suggestText = parser.text();
                } else if (parsefieldMatcher.match(fieldName, PREFIX_FIELD)) {
                    prefix = parser.text();
                } else if (parsefieldMatcher.match(fieldName, REGEX_FIELD)) {
                    regex = parser.text();
                } else {
                    throw new IllegalArgumentException("[suggestion] does not support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (suggestionName == null) {
                    throw new IllegalArgumentException("Suggestion must have name");
                }
                SuggestionBuilder<?> suggestParser = suggesters.getSuggestionPrototype(fieldName);
                if (suggestParser == null) {
                    throw new IllegalArgumentException("Suggester[" + fieldName + "] not supported");
                }
                suggestionBuilder = suggestParser.innerFromXContent(parseContext, suggestionName);
            }
        }
        if (suggestText != null) {
            suggestionBuilder.text(suggestText);
        }
        if (prefix != null) {
            suggestionBuilder.prefix(prefix);
        }
        if (regex != null) {
            suggestionBuilder.regex(regex);
        }
        return suggestionBuilder;
    }

    protected abstract SuggestionBuilder<T> innerFromXContent(QueryParseContext parseContext, String name) throws IOException;

    protected abstract SuggestionContext build(QueryShardContext context) throws IOException;

    /**
     * Transfers the text, prefix, regex, analyzer, fieldname, size and shard size settings from the
     * original {@link SuggestionBuilder} to the target {@link SuggestionContext}
     */
    protected void populateCommonFields(MapperService mapperService,
            SuggestionSearchContext.SuggestionContext suggestionContext) throws IOException {

        if (analyzer != null) {
            Analyzer luceneAnalyzer = mapperService.analysisService().analyzer(analyzer);
            if (luceneAnalyzer == null) {
                throw new IllegalArgumentException("Analyzer [" + luceneAnalyzer + "] doesn't exists");
            }
            suggestionContext.setAnalyzer(luceneAnalyzer);
        }

        if (fieldname != null) {
            suggestionContext.setField(fieldname);
        }

        if (size != null) {
            suggestionContext.setSize(size);
        }

        if (shardSize != null) {
            suggestionContext.setShardSize(shardSize);
        } else {
            // if no shard size is set in builder, use size (or at least 5)
            suggestionContext.setShardSize(Math.max(suggestionContext.getSize(), 5));
        }

        if (text != null) {
            suggestionContext.setText(BytesRefs.toBytesRef(text));
        }
        if (prefix != null) {
            suggestionContext.setPrefix(BytesRefs.toBytesRef(prefix));
        }
        if (regex != null) {
            suggestionContext.setRegex(BytesRefs.toBytesRef(regex));
        }
        if (text != null && prefix == null) {
            suggestionContext.setPrefix(BytesRefs.toBytesRef(text));
        } else if (text == null && prefix != null) {
            suggestionContext.setText(BytesRefs.toBytesRef(prefix));
        } else if (text == null && regex != null) {
            suggestionContext.setText(BytesRefs.toBytesRef(regex));
        }
    }

    private String getSuggesterName() {
        //default impl returns the same as writeable name, but we keep the distinction between the two just to make sure
        return getWriteableName();
    }


    /**
     * Sets from what field to fetch the candidate suggestions from. This is an
     * required option and needs to be set via this setter or
     * {@link org.elasticsearch.search.suggest.term.TermSuggestionBuilder#field(String)}
     * method
     */
    @SuppressWarnings("unchecked")
    public T field(String field) {
        this.fieldname = field;
        return (T)this;
    }

    /**
     * get the {@link #field()} parameter
     */
    public String field() {
        return this.fieldname;
    }

    /**
     * Sets the analyzer to analyse to suggest text with. Defaults to the search
     * analyzer of the suggest field.
     */
    @SuppressWarnings("unchecked")
    public T analyzer(String analyzer) {
        this.analyzer = analyzer;
        return (T)this;
    }

    /**
     * get the {@link #analyzer()} parameter
     */
    public String analyzer() {
        return this.analyzer;
    }

    /**
     * Sets the maximum suggestions to be returned per suggest text term.
     */
    @SuppressWarnings("unchecked")
    public T size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }
        this.size = size;
        return (T)this;
    }

    /**
     * get the {@link #size()} parameter
     */
    public Integer size() {
        return this.size;
    }

    /**
     * Sets the maximum number of suggested term to be retrieved from each
     * individual shard. During the reduce phase the only the top N suggestions
     * are returned based on the <code>size</code> option. Defaults to the
     * <code>size</code> option.
     * <p>
     * Setting this to a value higher than the `size` can be useful in order to
     * get a more accurate document frequency for suggested terms. Due to the
     * fact that terms are partitioned amongst shards, the shard level document
     * frequencies of suggestions may not be precise. Increasing this will make
     * these document frequencies more precise.
     */
    @SuppressWarnings("unchecked")
    public T shardSize(Integer shardSize) {
        this.shardSize = shardSize;
        return (T)this;
    }

    /**
     * get the {@link #shardSize()} parameter
     */
    public Integer shardSize() {
        return this.shardSize;
    }


    @Override
    public final T readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        T suggestionBuilder = doReadFrom(in, name);
        suggestionBuilder.fieldname = in.readOptionalString();
        suggestionBuilder.text = in.readOptionalString();
        suggestionBuilder.prefix = in.readOptionalString();
        suggestionBuilder.regex = in.readOptionalString();
        suggestionBuilder.analyzer = in.readOptionalString();
        suggestionBuilder.size = in.readOptionalVInt();
        suggestionBuilder.shardSize = in.readOptionalVInt();
        return suggestionBuilder;
    }

    /**
     * Subclass should return a new instance, reading itself from the input string
     * @param in the input string to read from
     * @param name the name of the suggestion (read from stream by {@link SuggestionBuilder}
     */
    protected abstract T doReadFrom(StreamInput in, String name) throws IOException;

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        doWriteTo(out);
        out.writeOptionalString(fieldname);
        out.writeOptionalString(text);
        out.writeOptionalString(prefix);
        out.writeOptionalString(regex);
        out.writeOptionalString(analyzer);
        out.writeOptionalVInt(size);
        out.writeOptionalVInt(shardSize);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        T other = (T) obj;
        return Objects.equals(name, other.name()) &&
               Objects.equals(text, other.text()) &&
               Objects.equals(prefix, other.prefix()) &&
               Objects.equals(regex, other.regex()) &&
               Objects.equals(fieldname, other.field()) &&
               Objects.equals(analyzer, other.analyzer()) &&
               Objects.equals(size, other.size()) &&
               Objects.equals(shardSize, other.shardSize()) &&
               doEquals(other);
    }

    /**
     * Indicates whether some other {@link SuggestionBuilder} of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(T other);

    @Override
    public final int hashCode() {
        return Objects.hash(name, text, prefix, regex, fieldname, analyzer, size, shardSize, doHashCode());
    }

    /**
     * HashCode for the subclass of {@link SuggestionBuilder} to implement.
     */
    protected abstract int doHashCode();

}
