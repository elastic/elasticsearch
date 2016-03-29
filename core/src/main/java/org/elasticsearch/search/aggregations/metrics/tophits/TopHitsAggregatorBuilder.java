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

package org.elasticsearch.search.aggregations.metrics.tophits;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder.ScriptField;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TopHitsAggregatorBuilder extends AggregatorBuilder<TopHitsAggregatorBuilder> {
    private int from = 0;
    private int size = 3;
    private boolean explain = false;
    private boolean version = false;
    private boolean trackScores = false;
    private List<SortBuilder<?>> sorts = null;
    private HighlightBuilder highlightBuilder;
    private List<String> fieldNames;
    private List<String> fieldDataFields;
    private List<ScriptField> scriptFields;
    private FetchSourceContext fetchSourceContext;

    public TopHitsAggregatorBuilder(String name) {
        super(name, InternalTopHits.TYPE);
    }

    /**
     * Read from a stream.
     */
    TopHitsAggregatorBuilder(StreamInput in) throws IOException {
        super(in, InternalTopHits.TYPE);
        explain = in.readBoolean();
        fetchSourceContext = FetchSourceContext.optionalReadFromStream(in);
        fieldDataFields = list(in.readOptionalStringArray());
        fieldNames = list(in.readOptionalStringArray());
        from = in.readVInt();
        highlightBuilder = in.readOptionalWriteable(HighlightBuilder::new);
        if (in.readBoolean()) {
            int size = in.readVInt();
            scriptFields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                scriptFields.add(ScriptField.PROTOTYPE.readFrom(in));
            }
        }
        size = in.readVInt();
        if (in.readBoolean()) {
            int size = in.readVInt();
            sorts = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                sorts.add(in.readSortBuilder());
            }
        }
        trackScores = in.readBoolean();
        version = in.readBoolean();
    }

    private <T> List<T> list(@Nullable T[] t) {
        if (t == null) {
            return null;
        }
        List<T> list = new ArrayList<>();
        Collections.addAll(list, t);
        return list;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(explain);
        FetchSourceContext.optionalWriteToStream(fetchSourceContext, out);
        out.writeOptionalStringArray(fieldDataFields == null ? null : fieldDataFields.toArray(new String[fieldDataFields.size()]));
        out.writeOptionalStringArray(fieldNames == null ? null : fieldNames.toArray(new String[fieldNames.size()]));
        out.writeVInt(from);
        out.writeOptionalWriteable(highlightBuilder);
        boolean hasScriptFields = scriptFields != null;
        out.writeBoolean(hasScriptFields);
        if (hasScriptFields) {
            out.writeVInt(scriptFields.size());
            for (ScriptField scriptField : scriptFields) {
                scriptField.writeTo(out);
            }
        }
        out.writeVInt(size);
        boolean hasSorts = sorts != null;
        out.writeBoolean(hasSorts);
        if (hasSorts) {
            out.writeVInt(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                out.writeSortBuilder(sort);
            }
        }
        out.writeBoolean(trackScores);
        out.writeBoolean(version);
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public TopHitsAggregatorBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[from] must be greater than or equal to 0. Found [" + from + "] in [" + name + "]");
        }
        this.from = from;
        return this;
    }

    /**
     * Gets the from index to start the search from.
     **/
    public int from() {
        return from;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public TopHitsAggregatorBuilder size(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("[size] must be greater than or equal to 0. Found [" + size + "] in [" + name + "]");
        }
        this.size = size;
        return this;
    }

    /**
     * Gets the number of search hits to return.
     */
    public int size() {
        return size;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param name
     *            The name of the field
     * @param order
     *            The sort ordering
     */
    public TopHitsAggregatorBuilder sort(String name, SortOrder order) {
        if (name == null) {
            throw new IllegalArgumentException("sort [name] must not be null: [" + name + "]");
        }
        if (order == null) {
            throw new IllegalArgumentException("sort [order] must not be null: [" + name + "]");
        }
        if (name.equals(ScoreSortBuilder.NAME)) {
            sort(SortBuilders.scoreSort().order(order));
        }
        sort(SortBuilders.fieldSort(name).order(order));
        return this;
    }

    /**
     * Add a sort against the given field name.
     *
     * @param name
     *            The name of the field to sort by
     */
    public TopHitsAggregatorBuilder sort(String name) {
        if (name == null) {
            throw new IllegalArgumentException("sort [name] must not be null: [" + name + "]");
        }
        if (name.equals(ScoreSortBuilder.NAME)) {
            sort(SortBuilders.scoreSort());
        }
        sort(SortBuilders.fieldSort(name));
        return this;
    }

    /**
     * Adds a sort builder.
     */
    public TopHitsAggregatorBuilder sort(SortBuilder<?> sort) {
        if (sort == null) {
            throw new IllegalArgumentException("[sort] must not be null: [" + name + "]");
        }
        if (sorts == null) {
                sorts = new ArrayList<>();
        }
        sorts.add(sort);
        return this;
    }

    /**
     * Adds a sort builder.
     */
    public TopHitsAggregatorBuilder sorts(List<SortBuilder<?>> sorts) {
        if (sorts == null) {
            throw new IllegalArgumentException("[sorts] must not be null: [" + name + "]");
        }
        if (this.sorts == null) {
            this.sorts = new ArrayList<>();
        }
        for (SortBuilder<?> sort : sorts) {
            this.sorts.add(sort);
        }
        return this;
    }

    /**
     * Gets the bytes representing the sort builders for this request.
     */
    public List<SortBuilder<?>> sorts() {
        return sorts;
    }

    /**
     * Adds highlight to perform as part of the search.
     */
    public TopHitsAggregatorBuilder highlighter(HighlightBuilder highlightBuilder) {
        if (highlightBuilder == null) {
            throw new IllegalArgumentException("[highlightBuilder] must not be null: [" + name + "]");
        }
        this.highlightBuilder = highlightBuilder;
        return this;
    }

    /**
     * Gets the hightlighter builder for this request.
     */
    public HighlightBuilder highlighter() {
        return highlightBuilder;
    }

    /**
     * Indicates whether the response should contain the stored _source for
     * every hit
     */
    public TopHitsAggregatorBuilder fetchSource(boolean fetch) {
        if (this.fetchSourceContext == null) {
            this.fetchSourceContext = new FetchSourceContext(fetch);
        } else {
            this.fetchSourceContext.fetchSource(fetch);
        }
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param include
     *            An optional include (optionally wildcarded) pattern to
     *            filter the returned _source
     * @param exclude
     *            An optional exclude (optionally wildcarded) pattern to
     *            filter the returned _source
     */
    public TopHitsAggregatorBuilder fetchSource(@Nullable String include, @Nullable String exclude) {
        fetchSource(include == null ? Strings.EMPTY_ARRAY : new String[] { include },
                exclude == null ? Strings.EMPTY_ARRAY : new String[] { exclude });
        return this;
    }

    /**
     * Indicate that _source should be returned with every hit, with an
     * "include" and/or "exclude" set which can include simple wildcard
     * elements.
     *
     * @param includes
     *            An optional list of include (optionally wildcarded)
     *            pattern to filter the returned _source
     * @param excludes
     *            An optional list of exclude (optionally wildcarded)
     *            pattern to filter the returned _source
     */
    public TopHitsAggregatorBuilder fetchSource(@Nullable String[] includes, @Nullable String[] excludes) {
        fetchSourceContext = new FetchSourceContext(includes, excludes);
        return this;
    }

    /**
     * Indicate how the _source should be fetched.
     */
    public TopHitsAggregatorBuilder fetchSource(@Nullable FetchSourceContext fetchSourceContext) {
        if (fetchSourceContext == null) {
            throw new IllegalArgumentException("[fetchSourceContext] must not be null: [" + name + "]");
        }
        this.fetchSourceContext = fetchSourceContext;
        return this;
    }

    /**
     * Gets the {@link FetchSourceContext} which defines how the _source
     * should be fetched.
     */
    public FetchSourceContext fetchSource() {
        return fetchSourceContext;
    }

    /**
     * Adds a field to load and return (note, it must be stored) as part of
     * the search request. If none are specified, the source of the document
     * will be return.
     */
    public TopHitsAggregatorBuilder field(String field) {
        if (field == null) {
            throw new IllegalArgumentException("[field] must not be null: [" + name + "]");
        }
        if (fieldNames == null) {
            fieldNames = new ArrayList<>();
        }
        fieldNames.add(field);
        return this;
    }

    /**
     * Sets the fields to load and return as part of the search request. If
     * none are specified, the source of the document will be returned.
     */
    public TopHitsAggregatorBuilder fields(List<String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("[fields] must not be null: [" + name + "]");
        }
        this.fieldNames = fields;
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be
     * returned per field.
     */
    public TopHitsAggregatorBuilder noFields() {
        this.fieldNames = Collections.emptyList();
        return this;
    }

    /**
     * Gets the fields to load and return as part of the search request.
     */
    public List<String> fields() {
        return fieldNames;
    }

    /**
     * Adds a field to load from the field data cache and return as part of
     * the search request.
     */
    public TopHitsAggregatorBuilder fieldDataField(String fieldDataField) {
        if (fieldDataField == null) {
            throw new IllegalArgumentException("[fieldDataField] must not be null: [" + name + "]");
        }
        if (fieldDataFields == null) {
            fieldDataFields = new ArrayList<>();
        }
        fieldDataFields.add(fieldDataField);
        return this;
    }

    /**
     * Adds fields to load from the field data cache and return as part of
     * the search request.
     */
    public TopHitsAggregatorBuilder fieldDataFields(List<String> fieldDataFields) {
        if (fieldDataFields == null) {
            throw new IllegalArgumentException("[fieldDataFields] must not be null: [" + name + "]");
        }
        if (this.fieldDataFields == null) {
            this.fieldDataFields = new ArrayList<>();
        }
        this.fieldDataFields.addAll(fieldDataFields);
        return this;
    }

    /**
     * Gets the field-data fields.
     */
    public List<String> fieldDataFields() {
        return fieldDataFields;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     */
    public TopHitsAggregatorBuilder scriptField(String name, Script script) {
        if (name == null) {
            throw new IllegalArgumentException("scriptField [name] must not be null: [" + name + "]");
        }
        if (script == null) {
            throw new IllegalArgumentException("scriptField [script] must not be null: [" + name + "]");
        }
        scriptField(name, script, false);
        return this;
    }

    /**
     * Adds a script field under the given name with the provided script.
     *
     * @param name
     *            The name of the field
     * @param script
     *            The script
     */
    public TopHitsAggregatorBuilder scriptField(String name, Script script, boolean ignoreFailure) {
        if (name == null) {
            throw new IllegalArgumentException("scriptField [name] must not be null: [" + name + "]");
        }
        if (script == null) {
            throw new IllegalArgumentException("scriptField [script] must not be null: [" + name + "]");
        }
        if (scriptFields == null) {
            scriptFields = new ArrayList<>();
        }
        scriptFields.add(new ScriptField(name, script, ignoreFailure));
        return this;
    }

    public TopHitsAggregatorBuilder scriptFields(List<ScriptField> scriptFields) {
        if (scriptFields == null) {
            throw new IllegalArgumentException("[scriptFields] must not be null: [" + name + "]");
        }
        if (this.scriptFields == null) {
            this.scriptFields = new ArrayList<>();
        }
        this.scriptFields.addAll(scriptFields);
        return this;
    }

    /**
     * Gets the script fields.
     */
    public List<ScriptField> scriptFields() {
        return scriptFields;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned
     * with an explanation of the hit (ranking).
     */
    public TopHitsAggregatorBuilder explain(boolean explain) {
        this.explain = explain;
        return this;
    }

    /**
     * Indicates whether each search hit will be returned with an
     * explanation of the hit (ranking)
     */
    public boolean explain() {
        return explain;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned
     * with a version associated with it.
     */
    public TopHitsAggregatorBuilder version(boolean version) {
        this.version = version;
        return this;
    }

    /**
     * Indicates whether the document's version will be included in the
     * search hits.
     */
    public boolean version() {
        return version;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well.
     * Defaults to <tt>false</tt>.
     */
    public TopHitsAggregatorBuilder trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    /**
     * Indicates whether scores will be tracked for this request.
     */
    public boolean trackScores() {
        return trackScores;
    }

    @Override
    public TopHitsAggregatorBuilder subAggregations(Builder subFactories) {
        throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
    }

    @Override
    protected TopHitsAggregatorFactory doBuild(AggregationContext context, AggregatorFactory<?> parent, Builder subfactoriesBuilder)
            throws IOException {
        return new TopHitsAggregatorFactory(name, type, from, size, explain, version, trackScores, sorts, highlightBuilder, fieldNames,
                fieldDataFields, scriptFields, fetchSourceContext, context, parent, subfactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SearchSourceBuilder.FROM_FIELD.getPreferredName(), from);
        builder.field(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), size);
        builder.field(SearchSourceBuilder.VERSION_FIELD.getPreferredName(), version);
        builder.field(SearchSourceBuilder.EXPLAIN_FIELD.getPreferredName(), explain);
        if (fetchSourceContext != null) {
            builder.field(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), fetchSourceContext);
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
        if (trackScores) {
            builder.field(SearchSourceBuilder.TRACK_SCORES_FIELD.getPreferredName(), true);
        }
        if (highlightBuilder != null) {
            builder.field(SearchSourceBuilder.HIGHLIGHT_FIELD.getPreferredName(), highlightBuilder);
        }
        builder.endObject();
        return builder;
    }

    protected int doHashCode() {
        return Objects.hash(explain, fetchSourceContext, fieldDataFields, fieldNames, from, highlightBuilder, scriptFields, size, sorts,
                trackScores, version);
    }

    @Override
    protected boolean doEquals(Object obj) {
        TopHitsAggregatorBuilder other = (TopHitsAggregatorBuilder) obj;
        return Objects.equals(explain, other.explain)
                && Objects.equals(fetchSourceContext, other.fetchSourceContext)
                && Objects.equals(fieldDataFields, other.fieldDataFields)
                && Objects.equals(fieldNames, other.fieldNames)
                && Objects.equals(from, other.from)
                && Objects.equals(highlightBuilder, other.highlightBuilder)
                && Objects.equals(scriptFields, other.scriptFields)
                && Objects.equals(size, other.size)
                && Objects.equals(sorts, other.sorts)
                && Objects.equals(trackScores, other.trackScores)
                && Objects.equals(version, other.version);
    }
}