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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
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
public final class ParentJoinFieldMapper extends ParametrizedFieldMapper {

    public static final String NAME = "join";
    public static final String CONTENT_TYPE = "join";

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

    private static void checkParentFields(String name, List<ParentIdFieldMapper> mappers) {
        Set<String> children = new HashSet<>();
        List<String> conflicts = new ArrayList<>();
        for (ParentIdFieldMapper mapper : mappers) {
            for (String child : mapper.getChildren()) {
                if (children.add(child) == false) {
                    conflicts.add("[" + child + "] cannot have multiple parents");
                }
            }
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalArgumentException("invalid definition for join field [" + name + "]:\n" + conflicts.toString());
        }
    }

    private static ParentJoinFieldMapper toType(FieldMapper in) {
        return (ParentJoinFieldMapper) in;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam("eager_global_ordinals", true,
            m -> m.fieldType().eagerGlobalOrdinals(), true);
        final Parameter<List<Relations>> relations = new Parameter<List<Relations>>("relations", true,
            Collections::emptyList, (n, c, o) -> Relations.parse(o), m -> toType(m).relations)
            .setMergeValidator(ParentJoinFieldMapper::checkRelationsConflicts);

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(eagerGlobalOrdinals, relations, meta);
        }

        @Override
        public ParentJoinFieldMapper build(BuilderContext context) {
            checkObjectOrNested(context.path(), name);
            final List<ParentIdFieldMapper> parentIdFields = new ArrayList<>();
            relations.get().stream()
                .map(relation -> new ParentIdFieldMapper.Builder(name + "#" + relation.parent,
                    relation.parent, relation.children))
                .map((parentBuilder) -> {
                    if (eagerGlobalOrdinals.get()) {
                        parentBuilder.eagerGlobalOrdinals(true);
                    }
                    return parentBuilder.build(context);
                })
                .forEach(parentIdFields::add);
            checkParentFields(name(), parentIdFields);
            MetaJoinFieldMapper unique = new MetaJoinFieldMapper.Builder(name).build(context);
            Joiner joiner = new Joiner(name(), relations.get());
            return new ParentJoinFieldMapper(name, new JoinFieldType(buildFullName(context), joiner, meta.get()),
                unique, Collections.unmodifiableList(parentIdFields),
                eagerGlobalOrdinals.getValue(), relations.get());
        }
    }

    public static TypeParser PARSER = new TypeParser((n, c) -> {
        checkIndexCompatibility(c.mapperService().getIndexSettings(), n);
        return new Builder(n);
    });

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
    private final MetaJoinFieldMapper uniqueFieldMapper;
    private final List<ParentIdFieldMapper> parentIdFields;
    private final boolean eagerGlobalOrdinals;
    private final List<Relations> relations;

    protected ParentJoinFieldMapper(String simpleName,
                                    MappedFieldType mappedFieldType,
                                    MetaJoinFieldMapper uniqueFieldMapper,
                                    List<ParentIdFieldMapper> parentIdFields,
                                    boolean eagerGlobalOrdinals, List<Relations> relations) {
        super(simpleName, mappedFieldType, MultiFields.empty(), CopyTo.empty());
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
        List<Mapper> mappers = new ArrayList<> (parentIdFields);
        mappers.add(uniqueFieldMapper);
        return mappers.iterator();
    }

    /**
     * Returns true if <code>name</code> is a parent name in the field.
     */
    public boolean hasParent(String name) {
        return parentIdFields.stream().anyMatch((mapper) -> name.equals(mapper.getParentName()));
    }

    /**
     * Returns true if <code>name</code> is a child name in the field.
     */
    public boolean hasChild(String name) {
        return parentIdFields.stream().anyMatch((mapper) -> mapper.getChildren().contains(name));
    }

    /**
     * Returns the parent Id field mapper associated with a parent <code>name</code>
     * if <code>isParent</code> is true and a child <code>name</code> otherwise.
     */
    public ParentIdFieldMapper getParentIdFieldMapper(String name, boolean isParent) {
        for (ParentIdFieldMapper mapper : parentIdFields) {
            if (isParent && name.equals(mapper.getParentName())) {
                return mapper;
            } else if (isParent == false && mapper.getChildren().contains(name)) {
                return mapper;
            }
        }
        return null;
    }

    private static boolean checkRelationsConflicts(List<Relations> previous, List<Relations> current, Conflicts conflicts) {
        Joiner pj = new Joiner("f", previous);
        Joiner cj = new Joiner("f", current);
        return pj.canMerge(cj, s -> conflicts.addConflict("relations", s));
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

        ParentIdFieldMapper parentIdField = getParentIdFieldMapper(name, true);
        ParentIdFieldMapper childParentIdField = getParentIdFieldMapper(name, false);
        if (parentIdField == null && childParentIdField == null) {
            throw new IllegalArgumentException("unknown join name [" + name + "] for field [" + name() + "]");
        }
        if (childParentIdField != null) {
            // Index the document as a child
            if (parent == null) {
                throw new IllegalArgumentException("[parent] is missing for join field [" + name() + "]");
            }
            if (context.sourceToParse().routing() == null) {
                throw new IllegalArgumentException("[routing] is missing for join field [" + name() + "]");
            }
            assert childParentIdField.getChildren().contains(name);
            ParseContext externalContext = context.createExternalValueContext(parent);
            childParentIdField.parse(externalContext);
        }
        if (parentIdField != null) {
            // Index the document as a parent
            assert parentIdField.getParentName().equals(name);
            ParseContext externalContext = context.createExternalValueContext(context.sourceToParse().id());
            parentIdField.parse(externalContext);
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
        for (ParentIdFieldMapper field : parentIdFields) {
            if (field.getChildren().size() == 1) {
                builder.field(field.getParentName(), field.getChildren().iterator().next());
            } else {
                builder.field(field.getParentName(), field.getChildren());
            }
        }
        builder.endObject();
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }
}
