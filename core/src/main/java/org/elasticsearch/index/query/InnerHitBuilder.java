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
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsContext;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsFetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;

public final class InnerHitBuilder extends ToXContentToBytes implements Writeable {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");
    public static final QueryBuilder DEFAULT_INNER_HIT_QUERY = new MatchAllQueryBuilder();

    private final static ObjectParser<InnerHitBuilder, QueryParseContext> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setFieldNames, SearchSourceBuilder.FIELDS_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setFieldDataFields, SearchSourceBuilder.FIELDDATA_FIELDS_FIELD);
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
                i.setFetchSourceContext(FetchSourceContext.parse(c));
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

    private int from;
    private int size = 3;
    private boolean explain;
    private boolean version;
    private boolean trackScores;

    private List<String> fieldNames;
    private QueryBuilder query = DEFAULT_INNER_HIT_QUERY;
    private List<SortBuilder<?>> sorts;
    private List<String> fieldDataFields;
    private Set<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private FetchSourceContext fetchSourceContext;
    private Map<String, InnerHitBuilder> childInnerHits;

    public InnerHitBuilder() {
    }

    /**
     * Read from a stream.
     */
    public InnerHitBuilder(StreamInput in) throws IOException {
        name = in.readOptionalString();
        nestedPath = in.readOptionalString();
        parentChildType = in.readOptionalString();
        from = in.readVInt();
        size = in.readVInt();
        explain = in.readBoolean();
        version = in.readBoolean();
        trackScores = in.readBoolean();
        fieldNames = (List<String>) in.readGenericValue();
        fieldDataFields = (List<String>) in.readGenericValue();
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(new ScriptField(in));
            }
        }
        fetchSourceContext = in.readOptionalStreamable(FetchSourceContext::new);
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

    private InnerHitBuilder(InnerHitBuilder other) {
        name = other.name;
        from = other.from;
        size = other.size;
        explain = other.explain;
        version = other.version;
        trackScores = other.trackScores;
        if (other.fieldNames != null) {
            fieldNames = new ArrayList<>(other.fieldNames);
        }
        if (other.fieldDataFields != null) {
            fieldDataFields = new ArrayList<>(other.fieldDataFields);
        }
        if (other.scriptFields != null) {
            scriptFields = new HashSet<>(other.scriptFields);
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


    InnerHitBuilder(InnerHitBuilder other, String nestedPath, QueryBuilder query) {
        this(other);
        this.query = query;
        this.nestedPath = nestedPath;
        if (name == null) {
            this.name = nestedPath;
        }
    }

    InnerHitBuilder(InnerHitBuilder other, QueryBuilder query, String parentChildType) {
        this(other);
        this.query = query;
        this.parentChildType = parentChildType;
        if (name == null) {
            this.name = parentChildType;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalString(nestedPath);
        out.writeOptionalString(parentChildType);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeBoolean(explain);
        out.writeBoolean(version);
        out.writeBoolean(trackScores);
        out.writeGenericValue(fieldNames);
        out.writeGenericValue(fieldDataFields);
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            for (ScriptField scriptField : scriptFields) {
                scriptField.writeTo(out);;
            }
        }
        out.writeOptionalStreamable(fetchSourceContext);
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
            for (Map.Entry<String, InnerHitBuilder> entry : childInnerHits.entrySet()) {
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

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public InnerHitBuilder setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public List<String> getFieldDataFields() {
        return fieldDataFields;
    }

    public InnerHitBuilder setFieldDataFields(List<String> fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public InnerHitBuilder addFieldDataField(String field) {
        if (fieldDataFields == null) {
            fieldDataFields = new ArrayList<>();
        }
        fieldDataFields.add(field);
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
            DocumentMapper documentMapper = queryShardContext.getMapperService().documentMapper(parentChildType);
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
            childInnerHits.put(entry.getKey(), childInnerHit);
        }
        innerHits.setChildInnerHits(childInnerHits);
    }

    private void setupInnerHitsContext(QueryShardContext context, InnerHitsContext.BaseInnerHits innerHitsContext) throws IOException {
        innerHitsContext.from(from);
        innerHitsContext.size(size);
        innerHitsContext.explain(explain);
        innerHitsContext.version(version);
        innerHitsContext.trackScores(trackScores);
        if (fieldNames != null) {
            if (fieldNames.isEmpty()) {
                innerHitsContext.emptyFieldNames();
            } else {
                for (String fieldName : fieldNames) {
                    innerHitsContext.fieldNames().add(fieldName);
                }
            }
        }
        if (fieldDataFields != null) {
            FieldDataFieldsContext fieldDataFieldsContext = innerHitsContext
                    .getFetchSubPhaseContext(FieldDataFieldsFetchSubPhase.CONTEXT_FACTORY);
            for (String field : fieldDataFields) {
                fieldDataFieldsContext.add(new FieldDataFieldsContext.FieldDataField(field));
            }
            fieldDataFieldsContext.setHitExecutionNeeded(true);
        }
        if (scriptFields != null) {
            for (ScriptField field : scriptFields) {
                SearchScript searchScript = innerHitsContext.scriptService().search(innerHitsContext.lookup(), field.script(),
                        ScriptContext.Standard.SEARCH, Collections.emptyMap(), context.getClusterState());
                innerHitsContext.scriptFields().add(new org.elasticsearch.search.fetch.script.ScriptFieldsContext.ScriptField(
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
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), trackScores);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext, params);
        }
        if (fieldNames != null) {
            if (fieldNames.size() == 1) {
                builder.field(SearchSourceBuilder.FIELDS_FIELD.getPreferredName(), fieldNames.get(0));
            } else {
                builder.startArray(SearchSourceBuilder.FIELDS_FIELD.getPreferredName());
                for (String fieldName : fieldNames) {
                    builder.value(fieldName);
                }
                builder.endArray();
            }
        }
        if (fieldDataFields != null) {
            builder.startArray(SearchSourceBuilder.FIELDDATA_FIELDS_FIELD.getPreferredName());
            for (String fieldDataField : fieldDataFields) {
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
                Objects.equals(from, that.from) &&
                Objects.equals(size, that.size) &&
                Objects.equals(explain, that.explain) &&
                Objects.equals(version, that.version) &&
                Objects.equals(trackScores, that.trackScores) &&
                Objects.equals(fieldNames, that.fieldNames) &&
                Objects.equals(fieldDataFields, that.fieldDataFields) &&
                Objects.equals(scriptFields, that.scriptFields) &&
                Objects.equals(fetchSourceContext, that.fetchSourceContext) &&
                Objects.equals(sorts, that.sorts) &&
                Objects.equals(highlightBuilder, that.highlightBuilder) &&
                Objects.equals(query, that.query) &&
                Objects.equals(childInnerHits, that.childInnerHits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, nestedPath, parentChildType, from, size, explain, version, trackScores, fieldNames,
                fieldDataFields, scriptFields, fetchSourceContext, sorts, highlightBuilder, query, childInnerHits);
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

}
