/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Base class for the different suggestion implementations.
 */
public abstract class SuggestionBuilder<T extends SuggestionBuilder<T>> implements VersionedNamedWriteable, ToXContentFragment {

    protected final String field;
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

    /**
     * Creates a new suggestion.
     * @param field field to execute suggestions on
     */
    protected SuggestionBuilder(String field) {
        Objects.requireNonNull(field, "suggestion requires a field name");
        if (field.isEmpty()) {
            throw new IllegalArgumentException("suggestion field name is empty");
        }
        this.field = field;
    }

    /**
     * internal copy constructor that copies over all class fields from second SuggestionBuilder except field name.
     */
    protected SuggestionBuilder(String field, SuggestionBuilder<?> in) {
        this(field);
        text = in.text;
        prefix = in.prefix;
        regex = in.regex;
        analyzer = in.analyzer;
        size = in.size;
        shardSize = in.shardSize;
    }

    /**
     * Read from a stream.
     */
    protected SuggestionBuilder(StreamInput in) throws IOException {
        field = in.readString();
        text = in.readOptionalString();
        prefix = in.readOptionalString();
        regex = in.readOptionalString();
        analyzer = in.readOptionalString();
        size = in.readOptionalVInt();
        shardSize = in.readOptionalVInt();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalString(text);
        out.writeOptionalString(prefix);
        out.writeOptionalString(regex);
        out.writeOptionalString(analyzer);
        out.writeOptionalVInt(size);
        out.writeOptionalVInt(shardSize);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * Same as in {@link SuggestBuilder#setGlobalText(String)}, but in the suggestion scope.
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

    /**
     * get the {@link #field()} parameter
     */
    public String field() {
        return this.field;
    }

    /**
     * Sets the analyzer to analyse to suggest text with. Defaults to the search
     * analyzer of the suggest field.
     */
    @SuppressWarnings("unchecked")
    public T analyzer(String analyzer) {
        this.analyzer = analyzer;
        return (T) this;
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
            throw new IllegalArgumentException("size must be positive");
        }
        this.size = size;
        return (T) this;
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
        return (T) this;
    }

    /**
     * get the {@link #shardSize()} parameter
     */
    public Integer shardSize() {
        return this.shardSize;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
        builder.field(FIELDNAME_FIELD.getPreferredName(), field);
        if (size != null) {
            builder.field(SIZE_FIELD.getPreferredName(), size);
        }
        if (shardSize != null) {
            builder.field(SHARDSIZE_FIELD.getPreferredName(), shardSize);
        }

        builder = innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    static SuggestionBuilder<?> fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        String suggestText = null;
        String prefix = null;
        String regex = null;
        SuggestionBuilder<?> suggestionBuilder = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (TEXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    suggestText = parser.text();
                } else if (PREFIX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    prefix = parser.text();
                } else if (REGEX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    regex = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "suggestion does not support [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                suggestionBuilder = parser.namedObject(SuggestionBuilder.class, currentFieldName, null);
            }
        }
        if (suggestionBuilder == null) {
            throw new ElasticsearchParseException("missing suggestion object");
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

    protected abstract SuggestionContext build(SearchExecutionContext context) throws IOException;

    /**
     * Transfers the text, prefix, regex, analyzer, field, size and shard size settings from the
     * original {@link SuggestionBuilder} to the target {@link SuggestionContext}
     */
    protected void populateCommonFields(SearchExecutionContext context, SuggestionSearchContext.SuggestionContext suggestionContext) {

        Objects.requireNonNull(field, "field must not be null");
        if (context.isFieldMapped(field) == false) {
            throw new IllegalArgumentException("no mapping found for field [" + field + "]");
        }
        MappedFieldType fieldType = context.getFieldType(field);
        if (analyzer == null) {
            suggestionContext.setAnalyzer(fieldType.getTextSearchInfo().getSearchAnalyzer());
        } else {
            Analyzer luceneAnalyzer = context.getIndexAnalyzers().get(analyzer);
            if (luceneAnalyzer == null) {
                throw new IllegalArgumentException("analyzer [" + analyzer + "] doesn't exists");
            }
            suggestionContext.setAnalyzer(luceneAnalyzer);
        }

        suggestionContext.setField(fieldType.name());

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
        // default impl returns the same as writeable name, but we keep the distinction between the two just to make sure
        return getWriteableName();
    }

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
        return Objects.equals(text, other.text())
            && Objects.equals(prefix, other.prefix())
            && Objects.equals(regex, other.regex())
            && Objects.equals(field, other.field())
            && Objects.equals(analyzer, other.analyzer())
            && Objects.equals(size, other.size())
            && Objects.equals(shardSize, other.shardSize())
            && doEquals(other);
    }

    /**
     * Indicates whether some other {@link SuggestionBuilder} of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(T other);

    @Override
    public final int hashCode() {
        return Objects.hash(text, prefix, regex, field, analyzer, size, shardSize, doHashCode());
    }

    /**
     * HashCode for the subclass of {@link SuggestionBuilder} to implement.
     */
    protected abstract int doHashCode();

}
