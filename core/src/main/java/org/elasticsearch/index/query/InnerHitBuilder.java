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
package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;

public final class InnerHitBuilder extends ToXContentToBytes implements Writeable {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField IGNORE_UNMAPPED = new ParseField("ignore_unmapped");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final QueryBuilder DEFAULT_INNER_HIT_QUERY = new MatchAllQueryBuilder();

    private static final ObjectParser<InnerHitBuilder, QueryParseContext> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareBoolean((innerHitBuilder, value) -> innerHitBuilder.ignoreUnmapped = value, IGNORE_UNMAPPED);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setStoredFieldNames, SearchSourceBuilder.STORED_FIELDS_FIELD);
        PARSER.declareField((p, i, c) -> {
            throw new ParsingException(p.getTokenLocation(), "The field [" +
                SearchSourceBuilder.FIELDS_FIELD + "] is no longer supported, please use [" +
                SearchSourceBuilder.STORED_FIELDS_FIELD + "] to retrieve stored fields or _source filtering " +
                "if the field is not stored");
        }, SearchSourceBuilder.FIELDS_FIELD, ObjectParser.ValueType.STRING_ARRAY);
        PARSER.declareStringArray(InnerHitBuilder::setDocValueFields, SearchSourceBuilder.DOCVALUE_FIELDS_FIELD);
        PARSER.declareField((p, i, c) -> {
            try {
                Set<ScriptField> scriptFields = new HashSet<>();
                for (XContentParser.Token token = p.nextToken(); token != END_OBJECT; token = p.nextToken()) {
                    scriptFields.add(new ScriptField(c));
                }
                i.setScriptFields(scriptFields);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner script definition", e);
            }
        }, SearchSourceBuilder.SCRIPT_FIELDS_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((p, i, c) -> i.setSorts(SortBuilder.fromXContent(c)), SearchSourceBuilder.SORT_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER.declareField((p, i, c) -> {
            try {
                i.setFetchSourceContext(FetchSourceContext.fromXContent(c.parser()));
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner _source definition", e);
            }
        }, SearchSourceBuilder._SOURCE_FIELD, ObjectParser.ValueType.OBJECT_OR_BOOLEAN);
        PARSER.declareObject(InnerHitBuilder::setHighlightBuilder, (p, c) -> HighlightBuilder.fromXContent(c),
                SearchSourceBuilder.HIGHLIGHT_FIELD);
        PARSER.declareObject(InnerHitBuilder::setChildInnerHits, (p, c) -> {
            try {
                Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
                String innerHitName = null;
                for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                    switch (token) {
                        case START_OBJECT:
                            InnerHitBuilder innerHitBuilder = InnerHitBuilder.fromXContent(c);
                            innerHitBuilder.setName(innerHitName);
                            innerHitBuilders.put(innerHitName, innerHitBuilder);
                            break;
                        case FIELD_NAME:
                            innerHitName = p.currentName();
                            break;
                        default:
                            throw new ParsingException(p.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] in ["
                                    + p.currentName() + "] but found [" + token + "]", p.getTokenLocation());
                    }
                }
                return innerHitBuilders;
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner query definition", e);
            }
        }, INNER_HITS_FIELD);
    }

    private String name;
    private String nestedPath;
    private String parentChildType;
    private boolean ignoreUnmapped;

    private int from;
    private int size = 3;
    private boolean explain;
    private boolean version;
    private boolean trackScores;

    private StoredFieldsContext storedFieldsContext;
    private QueryBuilder query = DEFAULT_INNER_HIT_QUERY;
    private List<SortBuilder<?>> sorts;
    private List<String> docValueFields;
    private Set<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private FetchSourceContext fetchSourceContext;
    private Map<String, InnerHitBuilder> childInnerHits;

    public InnerHitBuilder() {
    }

    private InnerHitBuilder(InnerHitBuilder other) {
        name = other.name;
        this.ignoreUnmapped = other.ignoreUnmapped;
        from = other.from;
        size = other.size;
        explain = other.explain;
        version = other.version;
        trackScores = other.trackScores;
        if (other.storedFieldsContext != null) {
            storedFieldsContext = new StoredFieldsContext(other.storedFieldsContext);
        }
        if (other.docValueFields != null) {
            docValueFields = new ArrayList<> (other.docValueFields);
        }
        if (other.scriptFields != null) {
            scriptFields = new HashSet<> (other.scriptFields);
        }
        if (other.fetchSourceContext != null) {
            fetchSourceContext = new FetchSourceContext(
                    other.fetchSourceContext.fetchSource(), other.fetchSourceContext.includes(), other.fetchSourceContext.excludes()
            );
        }
        if (other.sorts != null) {
            sorts = new ArrayList<>(other.sorts);
        }
        highlightBuilder = other.highlightBuilder;
        if (other.childInnerHits != null) {
            childInnerHits = new HashMap<>(other.childInnerHits);
        }
    }


    InnerHitBuilder(InnerHitBuilder other, String nestedPath, QueryBuilder query, boolean ignoreUnmapped) {
        this(other);
        this.query = query;
        this.nestedPath = nestedPath;
        this.ignoreUnmapped = ignoreUnmapped;
        if (name == null) {
            this.name = nestedPath;
        }
    }

    InnerHitBuilder(InnerHitBuilder other, QueryBuilder query, String parentChildType, boolean ignoreUnmapped) {
        this(other);
        this.query = query;
        this.parentChildType = parentChildType;
        this.ignoreUnmapped = ignoreUnmapped;
        if (name == null) {
            this.name = parentChildType;
        }
    }

    /**
     * Read from a stream.
     */
    public InnerHitBuilder(StreamInput in) throws IOException {
        name = in.readOptionalString();
        nestedPath = in.readOptionalString();
        parentChildType = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            ignoreUnmapped = in.readBoolean();
        }
        from = in.readVInt();
        size = in.readVInt();
        explain = in.readBoolean();
        version = in.readBoolean();
        trackScores = in.readBoolean();
        storedFieldsContext = in.readOptionalWriteable(StoredFieldsContext::new);
        docValueFields = (List<String>) in.readGenericValue();
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                sorts.add(in.readNamedWriteable(SortBuilder.class));
            }
        }
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        query = in.readNamedWriteable(QueryBuilder.class);
        if (in.readBoolean()) {
            int size = in.readVInt();
            childInnerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                childInnerHits.put(in.readString(), new InnerHitBuilder(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(nestedPath);
        out.writeOptionalString(parentChildType);
        if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            out.writeBoolean(ignoreUnmapped);
        }
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(trackScores);
        out.writeOptionalWriteable(storedFieldsContext);
        out.writeGenericValue(docValueFields);
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            Iterator<ScriptField> iterator = scriptFields.stream()
                    .sorted((a, b) -> a.fieldName().compareTo(b.fieldName())).iterator();
            while (iterator.hasNext()) {
                iterator.next().writeTo(out);
            }
        }
        out.writeOptionalWriteable(fetchSourceContext);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeNamedWriteable(sort);
            }
        }
        out.writeOptionalWriteable(highlightBuilder);
        out.writeNamedWriteable(query);
        boolean hasChildInnerHits = childInnerHits != null;
        out.writeBoolean(hasChildInnerHits);
        if (hasChildInnerHits) {
            out.writeVInt(childInnerHits.size());
            Iterator<Map.Entry<String, InnerHitBuilder>> iterator = childInnerHits.entrySet().stream()
                    .sorted((a, b) -> a.getKey().compareTo(b.getKey())).iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, InnerHitBuilder> entry = iterator.next();
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    public String getName() {
        return name;
    }

    public InnerHitBuilder setName(String name) {
        this.name = Objects.requireNonNull(name);
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

    public boolean isTrackScores() {
        return trackScores;
    }

    public InnerHitBuilder setTrackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Gets the stored fields to load and return.
     *
     * @deprecated Use {@link InnerHitBuilder#getStoredFieldsContext()} instead.
     */
    @Deprecated
    public List<String> getFieldNames() {
        return storedFieldsContext == null ? null : storedFieldsContext.fieldNames();
    }

    /**
     * Sets the stored fields to load and return.
     * If none are specified, the source of the document will be returned.
     *
     * @deprecated Use {@link InnerHitBuilder#setStoredFieldNames(List)} instead.
     */
    @Deprecated
    public InnerHitBuilder setFieldNames(List<String> fieldNames) {
        return setStoredFieldNames(fieldNames);
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
     *
     * @deprecated Use {@link InnerHitBuilder#getDocValueFields()} instead.
     */
    @Deprecated
    public List<String> getFieldDataFields() {
        return docValueFields;
    }

    /**
     * Sets the stored fields to load from the docvalue and return.
     *
     * @deprecated Use {@link InnerHitBuilder#setDocValueFields(List)} instead.
     */
    @Deprecated
    public InnerHitBuilder setFieldDataFields(List<String> fieldDataFields) {
        this.docValueFields = fieldDataFields;
        return this;
    }

    /**
     * Adds a field to load from the docvalue and return.
     *
     * @deprecated Use {@link InnerHitBuilder#addDocValueField(String)} instead.
     */
    @Deprecated
    public InnerHitBuilder addFieldDataField(String field) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(field);
        return this;
    }

    /**
     * Gets the docvalue fields.
     */
    public List<String> getDocValueFields() {
        return docValueFields;
    }

    /**
     * Sets the stored fields to load from the docvalue and return.
     */
    public InnerHitBuilder setDocValueFields(List<String> docValueFields) {
        this.docValueFields = docValueFields;
        return this;
    }

    /**
     * Adds a field to load from the docvalue and return.
     */
    public InnerHitBuilder addDocValueField(String field) {
        if (docValueFields == null) {
            docValueFields = new ArrayList<>();
        }
        docValueFields.add(field);
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

    QueryBuilder getQuery() {
        return query;
    }

    void setChildInnerHits(Map<String, InnerHitBuilder> childInnerHits) {
        this.childInnerHits = childInnerHits;
    }

    String getParentChildType() {
        return parentChildType;
    }

    String getNestedPath() {
        return nestedPath;
    }

    void addChildInnerHit(InnerHitBuilder innerHitBuilder) {
        if (childInnerHits == null) {
            childInnerHits = new HashMap<>();
        }
        this.childInnerHits.put(innerHitBuilder.getName(), innerHitBuilder);
    }

    public InnerHitsContext.BaseInnerHits build(SearchContext parentSearchContext,
                                                InnerHitsContext innerHitsContext) throws IOException {
        QueryShardContext queryShardContext = parentSearchContext.getQueryShardContext();
        if (nestedPath != null) {
            ObjectMapper nestedObjectMapper = queryShardContext.getObjectMapper(nestedPath);
            if (nestedObjectMapper == null) {
                if (ignoreUnmapped == false) {
                    throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + nestedPath + "]");
                } else {
                    return null;
                }
            }

            ObjectMapper parentObjectMapper = queryShardContext.nestedScope().nextLevel(nestedObjectMapper);
            InnerHitsContext.NestedInnerHits nestedInnerHits = new InnerHitsContext.NestedInnerHits(
                    name, parentSearchContext, parentObjectMapper, nestedObjectMapper
            );
            setupInnerHitsContext(queryShardContext, nestedInnerHits);
            if (childInnerHits != null) {
                buildChildInnerHits(parentSearchContext, nestedInnerHits);
            }
            queryShardContext.nestedScope().previousLevel();
            innerHitsContext.addInnerHitDefinition(nestedInnerHits);
            return nestedInnerHits;
        } else if (parentChildType != null) {
            DocumentMapper documentMapper = queryShardContext.documentMapper(parentChildType);
            if (documentMapper == null) {
                if (ignoreUnmapped == false) {
                    throw new IllegalStateException("[" + query.getName() + "] no mapping found for type [" + parentChildType + "]");
                } else {
                    return null;
                }
            }

            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(
                    name, parentSearchContext, queryShardContext.getMapperService(), documentMapper
            );
            setupInnerHitsContext(queryShardContext, parentChildInnerHits);
            if (childInnerHits != null) {
                buildChildInnerHits(parentSearchContext, parentChildInnerHits);
            }
            innerHitsContext.addInnerHitDefinition( parentChildInnerHits);
            return parentChildInnerHits;
        } else {
            throw new IllegalStateException("Neither a nested or parent/child inner hit");
        }
    }

    private void buildChildInnerHits(SearchContext parentSearchContext, InnerHitsContext.BaseInnerHits innerHits) throws IOException {
        Map<String, InnerHitsContext.BaseInnerHits> childInnerHits = new HashMap<>();
        for (Map.Entry<String, InnerHitBuilder> entry : this.childInnerHits.entrySet()) {
            InnerHitsContext.BaseInnerHits childInnerHit = entry.getValue().build(
                    parentSearchContext, new InnerHitsContext()
            );
            if (childInnerHit != null) {
                childInnerHits.put(entry.getKey(), childInnerHit);
            }
        }
        innerHits.setChildInnerHits(childInnerHits);
    }

    private void setupInnerHitsContext(QueryShardContext context, InnerHitsContext.BaseInnerHits innerHitsContext) throws IOException {
        innerHitsContext.from(from);
        innerHitsContext.size(size);
        innerHitsContext.explain(explain);
        innerHitsContext.version(version);
        innerHitsContext.trackScores(trackScores);
        if (storedFieldsContext != null) {
            innerHitsContext.storedFieldsContext(storedFieldsContext);
        }
        if (docValueFields != null) {
            innerHitsContext.docValueFieldsContext(new DocValueFieldsContext(docValueFields));
        }
        if (scriptFields != null) {
            for (ScriptField field : scriptFields) {
                SearchScript searchScript = innerHitsContext.getQueryShardContext().getSearchScript(field.script(),
                    ScriptContext.Standard.SEARCH);
                innerHitsContext.scriptFields().add(new org.elasticsearch.search.fetch.subphase.ScriptFieldsContext.ScriptField(
                        field.fieldName(), searchScript, field.ignoreFailure()));
            }
        }
        if (fetchSourceContext != null) {
            innerHitsContext.fetchSourceContext(fetchSourceContext);
        }
        if (sorts != null) {
            Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(sorts, context);
            if (optionalSort.isPresent()) {
                innerHitsContext.sort(optionalSort.get());
            }
        }
        if (highlightBuilder != null) {
            innerHitsContext.highlight(highlightBuilder.build(context));
        }
        ParsedQuery parsedQuery = new ParsedQuery(query.toQuery(context), context.copyNamedQueries());
        innerHitsContext.parsedQuery(parsedQuery);
    }

    public void inlineInnerHits(Map<String, InnerHitBuilder> innerHits) {
        InnerHitBuilder copy = new InnerHitBuilder(this);
        copy.parentChildType = this.parentChildType;
        copy.nestedPath = this.nestedPath;
        copy.query = this.query;
        innerHits.put(copy.getName(), copy);

        Map<String, InnerHitBuilder> childInnerHits = new HashMap<>();
        extractInnerHits(query, childInnerHits);
        if (childInnerHits.size() > 0) {
            copy.setChildInnerHits(childInnerHits);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        builder.field(IGNORE_UNMAPPED.getPreferredName(), ignoreUnmapped);
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), trackScores);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext, params);
        }
        if (storedFieldsContext != null) {
            storedFieldsContext.toXContent(SearchSourceBuilder.STORED_FIELDS_FIELD.getPreferredName(), builder);
        }
        if (docValueFields != null) {
            builder.startArray(SearchSourceBuilder.DOCVALUE_FIELDS_FIELD.getPreferredName());
            for (String fieldDataField : docValueFields) {
                builder.value(fieldDataField);
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
        if (childInnerHits != null) {
            builder.startObject(INNER_HITS_FIELD.getPreferredName());
            for (Map.Entry<String, InnerHitBuilder> entry : childInnerHits.entrySet()) {
                builder.field(entry.getKey(), entry.getValue(), params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InnerHitBuilder that = (InnerHitBuilder) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(nestedPath, that.nestedPath) &&
                Objects.equals(parentChildType, that.parentChildType) &&
                Objects.equals(ignoreUnmapped, that.ignoreUnmapped) &&
                Objects.equals(from, that.from) &&
                Objects.equals(size, that.size) &&
                Objects.equals(explain, that.explain) &&
                Objects.equals(version, that.version) &&
                Objects.equals(trackScores, that.trackScores) &&
                Objects.equals(storedFieldsContext, that.storedFieldsContext) &&
                Objects.equals(docValueFields, that.docValueFields) &&
                Objects.equals(scriptFields, that.scriptFields) &&
                Objects.equals(fetchSourceContext, that.fetchSourceContext) &&
                Objects.equals(sorts, that.sorts) &&
                Objects.equals(highlightBuilder, that.highlightBuilder) &&
                Objects.equals(query, that.query) &&
                Objects.equals(childInnerHits, that.childInnerHits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, nestedPath, parentChildType, ignoreUnmapped, from, size, explain, version, trackScores,
                storedFieldsContext, docValueFields, scriptFields, fetchSourceContext, sorts, highlightBuilder, query, childInnerHits);
    }

    public static InnerHitBuilder fromXContent(QueryParseContext context) throws IOException {
        return PARSER.parse(context.parser(), new InnerHitBuilder(), context);
    }

    public static void extractInnerHits(QueryBuilder query, Map<String, InnerHitBuilder> innerHitBuilders) {
        if (query instanceof AbstractQueryBuilder) {
            ((AbstractQueryBuilder) query).extractInnerHitBuilders(innerHitBuilders);
        } else {
            throw new IllegalStateException("provided query builder [" + query.getClass() +
                    "] class should inherit from AbstractQueryBuilder, but it doesn't");
        }
    }

    static InnerHitBuilder rewrite(InnerHitBuilder original, QueryBuilder rewrittenQuery) {
        if (original == null) {
            return null;
        }

        InnerHitBuilder copy = new InnerHitBuilder(original);
        copy.query = rewrittenQuery;
        copy.parentChildType = original.parentChildType;
        copy.nestedPath = original.nestedPath;
        return copy;
    }

}
