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
package org.elasticsearch.index.query.support;

import org.apache.lucene.search.Sort;
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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
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
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentParser.Token.END_OBJECT;

public final class InnerHitBuilder extends ToXContentToBytes implements Writeable {

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField NESTED_PATH_FIELD = new ParseField("path");
    public static final ParseField PARENT_CHILD_TYPE_FIELD = new ParseField("type");

    private final static ObjectParser<InnerHitBuilder, QueryParseContext> PARSER = new ObjectParser<>("inner_hits", InnerHitBuilder::new);

    static {
        PARSER.declareString(InnerHitBuilder::setName, NAME_FIELD);
        PARSER.declareString(InnerHitBuilder::setNestedPath, NESTED_PATH_FIELD);
        PARSER.declareString(InnerHitBuilder::setParentChildType, PARENT_CHILD_TYPE_FIELD);
        PARSER.declareInt(InnerHitBuilder::setFrom, SearchSourceBuilder.FROM_FIELD);
        PARSER.declareInt(InnerHitBuilder::setSize, SearchSourceBuilder.SIZE_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setExplain, SearchSourceBuilder.EXPLAIN_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setVersion, SearchSourceBuilder.VERSION_FIELD);
        PARSER.declareBoolean(InnerHitBuilder::setTrackScores, SearchSourceBuilder.TRACK_SCORES_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setFieldNames, SearchSourceBuilder.FIELDS_FIELD);
        PARSER.declareStringArray(InnerHitBuilder::setFieldDataFields, SearchSourceBuilder.FIELDDATA_FIELDS_FIELD);
        PARSER.declareField((p, i, c) -> {
            try {
                List<ScriptField> scriptFields = new ArrayList<>();
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
        PARSER.declareObject(InnerHitBuilder::setQuery, (p, c) ->{
            try {
                return c.parseInnerQueryBuilder();
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner query definition", e);
            }
        }, SearchSourceBuilder.QUERY_FIELD);
        PARSER.declareObject(InnerHitBuilder::setInnerHitsBuilder, (p, c) -> {
            try {
                return InnerHitsBuilder.fromXContent(c);
            } catch (IOException e) {
                throw new ParsingException(p.getTokenLocation(), "Could not parse inner query definition", e);
            }
        }, SearchSourceBuilder.INNER_HITS_FIELD);
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
    private QueryBuilder<?> query = new MatchAllQueryBuilder();
    private List<SortBuilder<?>> sorts;
    private List<String> fieldDataFields;
    private List<ScriptField> scriptFields;
    private HighlightBuilder highlightBuilder;
    private InnerHitsBuilder innerHitsBuilder;
    private FetchSourceContext fetchSourceContext;

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
            scriptFields = in.readList(ScriptField::new);
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
        innerHitsBuilder = in.readOptionalWriteable(InnerHitsBuilder::new);
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
            out.writeList(scriptFields);
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
        out.writeOptionalWriteable(innerHitsBuilder);
    }

    public InnerHitBuilder setParentChildType(String parentChildType) {
        this.parentChildType = parentChildType;
        return this;
    }

    public InnerHitBuilder setNestedPath(String nestedPath) {
        this.nestedPath = nestedPath;
        return this;
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

    public List<ScriptField> getScriptFields() {
        return scriptFields;
    }

    public InnerHitBuilder setScriptFields(List<ScriptField> scriptFields) {
        this.scriptFields = scriptFields;
        return this;
    }

    public InnerHitBuilder addScriptField(String name, Script script) {
        if (scriptFields == null) {
            scriptFields = new ArrayList<>();
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

    public InnerHitBuilder addSort(SortBuilder sort) {
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

    public QueryBuilder<?> getQuery() {
        return query;
    }

    public InnerHitBuilder setQuery(QueryBuilder<?> query) {
        this.query = Objects.requireNonNull(query);
        return this;
    }

    public InnerHitBuilder setInnerHitsBuilder(InnerHitsBuilder innerHitsBuilder) {
        this.innerHitsBuilder = innerHitsBuilder;
        return this;
    }

    public InnerHitsContext.BaseInnerHits buildInline(SearchContext parentSearchContext, QueryShardContext context) throws IOException {
        InnerHitsContext.BaseInnerHits innerHitsContext;
        if (nestedPath != null) {
            ObjectMapper nestedObjectMapper = context.getObjectMapper(nestedPath);
            ObjectMapper parentObjectMapper = context.nestedScope().getObjectMapper();
            innerHitsContext = new InnerHitsContext.NestedInnerHits(
                    name, parentSearchContext, parentObjectMapper, nestedObjectMapper
            );
        } else if (parentChildType != null) {
            DocumentMapper documentMapper = context.getMapperService().documentMapper(parentChildType);
            innerHitsContext = new InnerHitsContext.ParentChildInnerHits(
                    name, parentSearchContext, context.getMapperService(), documentMapper
            );
        } else {
            throw new IllegalStateException("Neither a nested or parent/child inner hit");
        }
        setupInnerHitsContext(context, innerHitsContext);
        return innerHitsContext;
    }

    /**
     * Top level inner hits are different than inline inner hits:
     * 1) Nesting. Top level inner hits can be hold nested inner hits, that why this method is recursive (via buildChildInnerHits)
     * 2) Top level inner hits query is an option, whereas with inline inner hits that is based on the nested, has_child
     *    or has_parent's inner query.
     *
     *  Because of these changes there are different methods for building inline (which is simpler) and top level inner
     *  hits. Also top level inner hits will soon be deprecated.
     */
    public InnerHitsContext.BaseInnerHits buildTopLevel(SearchContext parentSearchContext, QueryShardContext context,
                                                        InnerHitsContext innerHitsContext) throws IOException {
        if (nestedPath != null) {
            ObjectMapper nestedObjectMapper = context.getObjectMapper(nestedPath);
            ObjectMapper parentObjectMapper = context.nestedScope().nextLevel(nestedObjectMapper);
            InnerHitsContext.NestedInnerHits nestedInnerHits = new InnerHitsContext.NestedInnerHits(
                    name, parentSearchContext, parentObjectMapper, nestedObjectMapper
            );
            setupInnerHitsContext(context, nestedInnerHits);
            if (innerHitsBuilder != null) {
                buildChildInnerHits(parentSearchContext, context, nestedInnerHits);
            }
            context.nestedScope().previousLevel();
            innerHitsContext.addInnerHitDefinition(nestedInnerHits);
            return nestedInnerHits;
        } else if (parentChildType != null) {
            DocumentMapper documentMapper = context.getMapperService().documentMapper(parentChildType);
            InnerHitsContext.ParentChildInnerHits parentChildInnerHits = new InnerHitsContext.ParentChildInnerHits(
                    name, parentSearchContext, context.getMapperService(), documentMapper
            );
            setupInnerHitsContext(context, parentChildInnerHits);
            if (innerHitsBuilder != null) {
                buildChildInnerHits(parentSearchContext, context, parentChildInnerHits);
            }
            innerHitsContext.addInnerHitDefinition( parentChildInnerHits);
            return parentChildInnerHits;
        } else {
            throw new IllegalStateException("Neither a nested or parent/child inner hit");
        }
    }

    private void buildChildInnerHits(SearchContext parentSearchContext, QueryShardContext context,
                                     InnerHitsContext.BaseInnerHits innerHits) throws IOException {
        Map<String, InnerHitsContext.BaseInnerHits> childInnerHits = new HashMap<>();
        for (Map.Entry<String, InnerHitBuilder> entry : innerHitsBuilder.getInnerHitsBuilders().entrySet()) {
            InnerHitsContext.BaseInnerHits childInnerHit = entry.getValue().buildTopLevel(
                    parentSearchContext, context, new InnerHitsContext()
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
            Optional<Sort> optionalSort = SortBuilder.buildSort(sorts, context);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (nestedPath != null) {
            builder.field(NESTED_PATH_FIELD.getPreferredName(), nestedPath);
        }
        if (parentChildType != null) {
            builder.field(PARENT_CHILD_TYPE_FIELD.getPreferredName(), parentChildType);
        }
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
        builder.field(SearchSourceBuilder.QUERY_FIELD.getPreferredName(), query, params);
        if (innerHitsBuilder != null) {
            builder.field(SearchSourceBuilder.INNER_HITS_FIELD.getPreferredName(), innerHitsBuilder, params);
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
                Objects.equals(innerHitsBuilder, that.innerHitsBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, nestedPath, parentChildType, from, size, explain, version, trackScores, fieldNames,
                fieldDataFields, scriptFields, fetchSourceContext, sorts, highlightBuilder, query, innerHitsBuilder);
    }

    public static InnerHitBuilder fromXContent(QueryParseContext context) throws IOException {
        return PARSER.parse(context.parser(), new InnerHitBuilder(), context);
    }

}
