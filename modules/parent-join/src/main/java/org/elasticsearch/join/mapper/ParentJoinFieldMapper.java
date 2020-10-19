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

package org.elasticsearch.join.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SourceValueFetcher;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} that creates hierarchical joins (parent-join) between documents in the same index.
 * Only one parent-join field can be defined per index. The verification of this assumption is done
 * through the {@link MetaJoinFieldMapper} which declares a meta field called "_parent_join".
 * This field is only used to ensure that there is a single parent-join field defined in the mapping and
 * cannot be used to index or query any data.
 */
public final class ParentJoinFieldMapper extends FieldMapper {
    public static final String NAME = "join";
    public static final String CONTENT_TYPE = "join";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    private static void checkIndexCompatibility(IndexSettings settings, String name) {
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            throw new IllegalStateException("cannot create join field [" + name + "] " +
                "for the partitioned index " + "[" + settings.getIndex().getName() + "]");
        }
    }

    private static void checkObjectOrNested(ContentPath path, String name) {
        if (path.pathAsText(name).contains(".")) {
            throw new IllegalArgumentException("join field [" + path.pathAsText(name) + "] " +
                "cannot be added inside an object or in a multi-field");
        }
    }

    public static class Builder extends FieldMapper.Builder {
        final List<Relations> relations = new ArrayList<>();
        boolean eagerGlobalOrdinals = true;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
        }

        public Builder addRelation(String parent, Set<String> children) {
            relations.add(new Relations(parent, children));
            return this;
        }

        public Builder eagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
            return this;
        }

        @Override
        public ParentJoinFieldMapper build(BuilderContext context) {
            checkObjectOrNested(context.path(), name);
            final Map<String, ParentIdFieldMapper> parentIdFields = new HashMap<>();
            relations.stream()
                .map(relation -> {
                    ParentIdFieldMapper.Builder builder = new ParentIdFieldMapper.Builder(name() + "#" + relation.parent);
                    if (eagerGlobalOrdinals) {
                        builder.eagerGlobalOrdinals(true);
                    }
                    return builder.build(context);
                })
                .forEach(mapper -> parentIdFields.put(mapper.name(), mapper));
            MetaJoinFieldMapper unique = new MetaJoinFieldMapper.Builder(name).build(context);
            Joiner joiner = new Joiner(name(), relations);
            return new ParentJoinFieldMapper(name, fieldType, new JoinFieldType(buildFullName(context), joiner, meta),
                unique, Collections.unmodifiableMap(parentIdFields), eagerGlobalOrdinals, relations);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final IndexSettings indexSettings = parserContext.getIndexSettings();
            checkIndexCompatibility(indexSettings, name);

            Builder builder = new Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                if ("type".equals(entry.getKey())) {
                    continue;
                }
                if ("eager_global_ordinals".equals(entry.getKey())) {
                    builder.eagerGlobalOrdinals(XContentMapValues.nodeBooleanValue(entry.getValue(), "eager_global_ordinals"));
                    iterator.remove();
                    continue;
                }
                if ("relations".equals(entry.getKey())) {
                    Map<String, Object> relations = XContentMapValues.nodeMapValue(entry.getValue(), "relations");
                    for (Iterator<Map.Entry<String, Object>> relIt = relations.entrySet().iterator(); relIt.hasNext(); ) {
                        Map.Entry<String, Object> relation = relIt.next();
                        final String parent = relation.getKey();
                        Set<String> children;
                        if (XContentMapValues.isArray(relation.getValue())) {
                            children = new HashSet<>(Arrays.asList(XContentMapValues.nodeStringArrayValue(relation.getValue())));
                        } else {
                            children = Collections.singleton(relation.getValue().toString());
                        }
                        builder.addRelation(parent, children);
                    }
                    iterator.remove();
                }
            }
            return builder;
        }
    }

    public static final class JoinFieldType extends StringFieldType {

        private final Joiner joiner;

        private JoinFieldType(String name, Joiner joiner, Map<String, String> meta) {
            super(name, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.joiner = joiner;
        }

        Joiner getJoiner() {
            return joiner;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), mapperService, format);
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }
    }

    // The meta field that ensures that there is no other parent-join in the mapping
    private MetaJoinFieldMapper uniqueFieldMapper;
    private Map<String, ParentIdFieldMapper> parentIdFields;
    private List<Relations> relations;
    private boolean eagerGlobalOrdinals;

    protected ParentJoinFieldMapper(String simpleName,
                                    FieldType fieldType,
                                    MappedFieldType mappedFieldType,
                                    MetaJoinFieldMapper uniqueFieldMapper,
                                    Map<String, ParentIdFieldMapper> parentIdFields,
                                    boolean eagerGlobalOrdinals, List<Relations> relations) {
        super(simpleName, fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        this.parentIdFields = parentIdFields;
        this.uniqueFieldMapper = uniqueFieldMapper;
        this.eagerGlobalOrdinals = eagerGlobalOrdinals;
        this.relations = relations;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected ParentJoinFieldMapper clone() {
        return (ParentJoinFieldMapper) super.clone();
    }

    @Override
    public JoinFieldType fieldType() {
        return (JoinFieldType) super.fieldType();
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> mappers = new ArrayList<>(parentIdFields.values());
        mappers.add(uniqueFieldMapper);
        return mappers.iterator();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        ParentJoinFieldMapper joinMergeWith = (ParentJoinFieldMapper) other;
        if (fieldType().joiner.canMerge(joinMergeWith.fieldType().joiner, conflicts::add) == false) {
            return;
        }
        this.eagerGlobalOrdinals = joinMergeWith.eagerGlobalOrdinals;
        this.parentIdFields = joinMergeWith.parentIdFields;
        this.uniqueFieldMapper = (MetaJoinFieldMapper) uniqueFieldMapper.merge(joinMergeWith.uniqueFieldMapper);
        this.relations = joinMergeWith.relations;
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        throw new UnsupportedOperationException("parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        context.path().add(simpleName());
        XContentParser.Token token = context.parser().currentToken();
        String name = null;
        String parent = null;
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = context.parser().nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = context.parser().currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("name".equals(currentFieldName)) {
                        name = context.parser().text();
                    } else if ("parent".equals(currentFieldName)) {
                        parent = context.parser().text();
                    } else {
                        throw new IllegalArgumentException("unknown field name [" + currentFieldName + "] in join field [" + name() + "]");
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("parent".equals(currentFieldName)) {
                        parent = context.parser().numberValue().toString();
                    } else {
                        throw new IllegalArgumentException("unknown field name [" + currentFieldName + "] in join field [" + name() + "]");
                    }
                }
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            name = context.parser().text();
            parent = null;
        } else {
            throw new IllegalStateException("[" + name  + "] expected START_OBJECT or VALUE_STRING but was: " + token);
        }

        if (fieldType().joiner.knownRelation(name) == false) {
            throw new IllegalArgumentException("unknown join name [" + name + "] for field [" + name() + "]");
        }
        if (fieldType().joiner.childTypeExists(name)) {
            // Index the document as a child
            if (parent == null) {
                throw new IllegalArgumentException("[parent] is missing for join field [" + name() + "]");
            }
            if (context.sourceToParse().routing() == null) {
                throw new IllegalArgumentException("[routing] is missing for join field [" + name() + "]");
            }
            ParseContext externalContext = context.createExternalValueContext(parent);
            String fieldName = fieldType().joiner.parentJoinField(name);
            parentIdFields.get(fieldName).parse(externalContext);
        }
        if (fieldType().joiner.parentTypeExists(name)) {
            // Index the document as a parent
            ParseContext externalContext = context.createExternalValueContext(context.sourceToParse().id());
            String fieldName = fieldType().joiner.childJoinField(name);
            parentIdFields.get(fieldName).parse(externalContext);
        }

        BytesRef binaryValue = new BytesRef(name);
        Field field = new Field(fieldType().name(), binaryValue, fieldType);
        context.doc().add(field);
        context.doc().add(new SortedDocValuesField(fieldType().name(), binaryValue));
        context.path().remove();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        builder.field("eager_global_ordinals", eagerGlobalOrdinals);
        builder.startObject("relations");
        for (Relations relation : relations) {
            if (relation.children.size() == 1) {
                builder.field(relation.parent, relation.children.iterator().next());
            } else {
                builder.field(relation.parent, relation.children);
            }
        }
        builder.endObject();
    }

}
