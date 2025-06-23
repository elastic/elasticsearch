/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentParser.Token.END_OBJECT;

public final class InnerHitBuilder implements Writeable, ToXContentObject {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");
    public static final ParseField COLLAPSE_FIELD = new ParseField("collapse");
    public static final ParseField FIELD_FIELD = new ParseField("field");

    public static final int DEFAULT_FROM = 0;
    public static final int DEFAULT_SIZE = 3;
    private static final boolean DEFAULT_IGNORE_UNAMPPED = false;
    private static final boolean DEFAULT_VERSION = false;
    private static final boolean DEFAULT_SEQ_NO_AND_PRIMARY_TERM = false;
    private static final boolean DEFAULT_EXPLAIN = false;
    private static final boolean DEFAULT_TRACK_SCORES = false;

    private static final ObjectParser<InnerHitBuilder, Void> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareBoolean((innerHitBuilder, value) -> innerHitBuilder.ignoreUnmapped = value, IGNORE_UNMAPPED);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setSeqNoAndPrimaryTerm, SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setStoredFieldNames, SearchSourceBuilder.STORED_FIELDS_FIELD);
        PARSER.declareObjectArray(
            InnerHitBuilder::setDocValueFields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.DOCVALUE_FIELDS_FIELD
        );
        PARSER.declareObjectArray(
            InnerHitBuilder::setFetchFields,
            (p, c) -> FieldAndFormat.fromXContent(p),
            SearchSourceBuilder.FETCH_FIELDS_FIELD
        );
        PARSER.declareField((p, i, c) -> {
            try {
                Set<ScriptField> scriptFields = new HashSet<>();
                for (XContentParser.Token token = p.nextToken(); token != END_OBJECT; token = p.nextToken()) {
                    scriptFields.add(new ScriptField(p));
                }
                i.setScriptFields(scriptFields);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner script definition", e);
            }
        }, SearchSourceBuilder.SCRIPT_FIELDS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField(
            (p, i, c) -> i.setSorts(SortBuilder.fromXContent(p)),
            SearchSourceBuilder.SORT_FIELD,
            ObjectParser.ValueType.OBJECT_ARRAY
        );
        PARSER.declareField((p, i, c) -> {
            try {
                i.setFetchSourceContext(FetchSourceContext.fromXContent(p));
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner _source definition", e);
            }
        }, SearchSourceBuilder._SOURCE_FIELD, ObjectParser.ValueType.OBJECT_ARRAY_BOOLEAN_OR_STRING);
        PARSER.declareObject(
            InnerHitBuilder::setHighlightBuilder,
            (p, c) -> HighlightBuilder.fromXContent(p),
            SearchSourceBuilder.HIGHLIGHT_FIELD
        );
        PARSER.declareField((parser, builder, context) -> {
            Boolean isParsedCorrectly = false;
            String field;
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                    if (FIELD_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
                            field = parser.text();
                            if (parser.nextToken() == XContentParser.Token.END_OBJECT) {
                                isParsedCorrectly = true;
                                CollapseBuilder cb = new CollapseBuilder(field);
                                builder.setInnerCollapse(cb);
                            }
                        }
                    }
                }
            }
            if (isParsedCorrectly == false) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid token in the inner collapse");
            }

        }, COLLAPSE_FIELD, ObjectParser.ValueType.OBJECT);
    }
    private String name;
    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNAMPPED;

    private int from = DEFAULT_FROM;
    private int size = DEFAULT_SIZE;
    private boolean explain = DEFAULT_EXPLAIN;
    private boolean version = DEFAULT_VERSION;
    private boolean seqNoAndPrimaryTerm = DEFAULT_SEQ_NO_AND_PRIMARY_TERM;
    private boolean trackScores = DEFAULT_TRACK_SCORES;

    private StoredFieldsContext storedFieldsContext;
    private List<SortBuilder<?>> sorts;
    private List<FieldAndFormat> docValueFields;
    private Set<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private FetchSourceContext fetchSourceContext;
    private List<FieldAndFormat> fetchFields;
    private CollapseBuilder innerCollapseBuilder = null;

    public InnerHitBuilder() {
        this.name = null;
    }

    public InnerHitBuilder(String name) {
        this.name = name;
    }

    /**
     * Read from a stream.
     */
    public InnerHitBuilder(StreamInput in) throws IOException {
        name = in.readOptionalString();
        ignoreUnmapped = in.readBoolean();
        from = in.readVInt();
        size = in.readVInt();
        explain = in.readBoolean();
        version = in.readBoolean();
        seqNoAndPrimaryTerm = in.readBoolean();
        trackScores = in.readBoolean();
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        docValueFields = in.readBoolean() ? in.readCollectionAsList(FieldAndFormat::new) : null;
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = Sets.newHashSetWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::readFrom);
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        this.innerCollapseBuilder = in.readOptionalWriteable(CollapseBuilder::new);

        fetchFields = in.readOptionalCollectionAsList(FieldAndFormat::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeBoolean(ignoreUnmapped);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(seqNoAndPrimaryTerm);
        out.writeBoolean(trackScores);
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeBoolean(docValueFields != null);
        if (docValueFields != null) {
            out.writeCollection(docValueFields);
        }
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            Iterator<ScriptField> iterator = scriptFields.stream().sorted(Comparator.comparing(ScriptField::fieldName)).iterator();
            while (iterator.hasNext()) {
                iterator.next().writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeNamedWriteableCollection(sorts);
        }
        out.writeOptionalWriteable(highlightBuilder);
        out.writeOptionalWriteable(innerCollapseBuilder);
        out.writeOptionalCollection(fetchFields);
    }

    public String getName() {
        return name;
    }

    public InnerHitBuilder setName(String name) {
        this.name = Objects.requireNonNull(name);
        return this;
    }

    public InnerHitBuilder setIgnoreUnmapped(boolean value) {
        this.ignoreUnmapped = value;
        return this;
    }

    /**
     * Whether to include inner hits in the search response hits if required mappings is missing
     */
    public boolean isIgnoreUnmapped() {
        return ignoreUnmapped;
    }

    public int getFrom() {
        return from;
    }

    public InnerHitBuilder setFrom(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("illegal from value, at least 0 or higher");
        }
        this.from = from;
        return this;
    }

    public int getSize() {
        return size;
    }

    public InnerHitBuilder setSize(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("illegal size value, at least 0 or higher");
        }
        this.size = size;
        return this;
    }

    public boolean isExplain() {
        return explain;
    }

    public InnerHitBuilder setExplain(boolean explain) {
        this.explain = explain;
        return this;
    }

    public boolean isVersion() {
        return version;
    }

    public InnerHitBuilder setVersion(boolean version) {
        this.version = version;
        return this;
    }

    public boolean isSeqNoAndPrimaryTerm() {
        return seqNoAndPrimaryTerm;
    }

    public InnerHitBuilder setSeqNoAndPrimaryTerm(boolean seqNoAndPrimaryTerm) {
        this.seqNoAndPrimaryTerm = seqNoAndPrimaryTerm;
        return this;
    }

    public boolean isTrackScores() {
        return trackScores;
    }

    public InnerHitBuilder setTrackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Gets the stored fields context.
     */
    public StoredFieldsContext getStoredFieldsContext() {
        return storedFieldsContext;
    }

    /**
     * Sets the stored fields to load and return.
     * If none are specified, the source of the document will be returned.
     */
    public InnerHitBuilder setStoredFieldNames(List<String> fieldNames) {
        if (storedFieldsContext == null) {
            storedFieldsContext = StoredFieldsContext.fromList(fieldNames);
        } else {
            storedFieldsContext.addFieldNames(fieldNames);
        }
        return this;
    }

    /**
     * Gets the docvalue fields.
     */
    public List<FieldAndFormat> getDocValueFields() {
        return docValueFields;
    }

    /**
     * Sets the stored fields to load from the docvalue and return.
     */
    public InnerHitBuilder setDocValueFields(List<FieldAndFormat> docValueFields) {
        this.docValueFields = docValueFields;
        return this;
    }

    /**
     * Adds a field to load from the docvalue and return.
     */
    public InnerHitBuilder addDocValueField(String field, String format) {
        if (docValueFields == null || docValueFields.isEmpty()) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(new FieldAndFormat(field, format));
        return this;
    }

    /**
     * Adds a field to load from doc values and return.
     */
    public InnerHitBuilder addDocValueField(String field) {
        return addDocValueField(field, null);
    }

    /**
     * Gets the fields to load and return as part of the search request.
     */
    public List<FieldAndFormat> getFetchFields() {
        return fetchFields;
    }

    /**
     * Sets the stored fields to load and return as part of the search request.
     */
    public InnerHitBuilder setFetchFields(List<FieldAndFormat> fetchFields) {
        this.fetchFields = fetchFields;
        return this;
    }

    /**
     * Adds a field to load and return as part of the search request.
     */
    public InnerHitBuilder addFetchField(String name) {
        return addFetchField(name, null);
    }

    /**
     * Adds a field to load and return as part of the search request.
     * @param name the field name.
     * @param format an optional format string used when formatting values, for example a date format.
     */
    public InnerHitBuilder addFetchField(String name, @Nullable String format) {
        return addFetchField(name, format, null);
    }

    /**
     * Adds a field to load and return as part of the search request.
     * @param name the field name.
     * @param format an optional format string used when formatting values, for example a date format.
     * @param includeUnmapped whether unmapped fields should be returned as well
     */
    public InnerHitBuilder addFetchField(String name, @Nullable String format, Boolean includeUnmapped) {
        if (fetchFields == null || fetchFields.isEmpty()) {
            fetchFields = new ArrayList<>();
        }
        fetchFields.add(new FieldAndFormat(name, format, includeUnmapped));
        return this;
    }

    public Set<ScriptField> getScriptFields() {
        return scriptFields;
    }

    public InnerHitBuilder setScriptFields(Set<ScriptField> scriptFields) {
        this.scriptFields = scriptFields;
        return this;
    }

    public InnerHitBuilder addScriptField(String name, Script script) {
        if (scriptFields == null) {
            scriptFields = new HashSet<>();
        }
        scriptFields.add(new ScriptField(name, script, false));
        return this;
    }

    public FetchSourceContext getFetchSourceContext() {
        return fetchSourceContext;
    }

    public InnerHitBuilder setFetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    public List<SortBuilder<?>> getSorts() {
        return sorts;
    }

    public InnerHitBuilder setSorts(List<SortBuilder<?>> sorts) {
        this.sorts = sorts;
        return this;
    }

    public InnerHitBuilder addSort(SortBuilder<?> sort) {
        if (sorts == null) {
            sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    public HighlightBuilder getHighlightBuilder() {
        return highlightBuilder;
    }

    public InnerHitBuilder setHighlightBuilder(HighlightBuilder highlightBuilder) {
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    public InnerHitBuilder setInnerCollapse(CollapseBuilder innerCollapseBuilder) {
        this.innerCollapseBuilder = innerCollapseBuilder;
        return this;
    }

    public CollapseBuilder getInnerCollapseBuilder() {
        return innerCollapseBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        if (ignoreUnmapped != DEFAULT_IGNORE_UNAMPPED) {
            builder.field(IGNORE_UNMAPPED.getPreferredName(), ignoreUnmapped);
        }
        if (from != DEFAULT_FROM) {
            builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        }
        if (size != DEFAULT_SIZE) {
            builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        }
        if (version != DEFAULT_VERSION) {
            builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        }
        if (seqNoAndPrimaryTerm != DEFAULT_SEQ_NO_AND_PRIMARY_TERM) {
            builder.field(SearchSourceBuilder.SEQ_NO_PRIMARY_TERM_FIELD.getPreferredName(), seqNoAndPrimaryTerm);
        }
        if (explain != DEFAULT_EXPLAIN) {
            builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        }
        if (trackScores != DEFAULT_TRACK_SCORES) {
            builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), trackScores);
        }
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext, params);
        }
        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), builder);
        }
        if (docValueFields != null) {
            builder.startArray(SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : docValueFields) {
                docValueField.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (fetchFields != null) {
            builder.startArray(SearchSourceBuilder.FETCH_FIELDS_FIELD.getPreferredName());
            for (FieldAndFormat docValueField : fetchFields) {
                docValueField.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (scriptFields != null) {
            builder.startObject(SearchSourceBuilder.SCRIPT_FIELDS_FIELD.getPreferredName());
            for (ScriptField scriptField : scriptFields) {
                scriptField.toXContent(builder, params);
            }
            builder.endObject();
        }
        if (sorts != null) {
            builder.startArray(SearchSourceBuilder.SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sorts) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder, params);
        }
        if (innerCollapseBuilder != null) {
            builder.field(COLLAPSE_FIELD.getPreferredName(), innerCollapseBuilder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InnerHitBuilder that = (InnerHitBuilder) o;
        return ignoreUnmapped == that.ignoreUnmapped
            && from == that.from
            && size == that.size
            && explain == that.explain
            && version == that.version
            && seqNoAndPrimaryTerm == that.seqNoAndPrimaryTerm
            && trackScores == that.trackScores
            && Objects.equals(name, that.name)
            && Objects.equals(storedFieldsContext, that.storedFieldsContext)
            && Objects.equals(sorts, that.sorts)
            && Objects.equals(docValueFields, that.docValueFields)
            && Objects.equals(scriptFields, that.scriptFields)
            && Objects.equals(highlightBuilder, that.highlightBuilder)
            && Objects.equals(fetchSourceContext, that.fetchSourceContext)
            && Objects.equals(fetchFields, that.fetchFields)
            && Objects.equals(innerCollapseBuilder, that.innerCollapseBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            ignoreUnmapped,
            from,
            size,
            explain,
            version,
            seqNoAndPrimaryTerm,
            trackScores,
            storedFieldsContext,
            sorts,
            docValueFields,
            scriptFields,
            highlightBuilder,
            fetchSourceContext,
            fetchFields,
            innerCollapseBuilder
        );
    }

    public static InnerHitBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new InnerHitBuilder(), null);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
