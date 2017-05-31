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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.StringFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FieldMapper} that creates hierarchical joins (parent-join) between documents in the same index.
 * TODO Should be restricted to a single join field per index
 */
public final class ParentJoinFieldMapper extends FieldMapper {
    public static final String NAME = "join";
    public static final String CONTENT_TYPE = "join";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new JoinFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    static String getParentIdFieldName(String joinFieldName, String parentName) {
        return joinFieldName + "#" + parentName;
    }

    static void checkPreConditions(Version indexCreatedVersion, ContentPath path, String name) {
        if (indexCreatedVersion.before(Version.V_6_0_0_alpha2)) {
            throw new IllegalStateException("unable to create join field [" + name +
                "] for index created before " + Version.V_6_0_0_alpha2);
        }

        if (path.pathAsText(name).contains(".")) {
            throw new IllegalArgumentException("join field [" + path.pathAsText(name) + "] " +
                "cannot be added inside an object or in a multi-field");
        }
    }

    static void checkParentFields(String name, List<ParentIdFieldMapper> mappers) {
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

    static void checkDuplicateJoinFields(ParseContext.Document doc) {
        if (doc.getFields().stream().anyMatch((m) -> m.fieldType() instanceof JoinFieldType)) {
            throw new IllegalStateException("cannot have two join fields in the same document");
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, ParentJoinFieldMapper> {
        final List<ParentIdFieldMapper.Builder> parentIdFieldBuilders = new ArrayList<>();

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public JoinFieldType fieldType() {
            return (JoinFieldType) super.fieldType();
        }

        public Builder addParent(String parent, Set<String> children) {
            String parentIdFieldName = getParentIdFieldName(name, parent);
            parentIdFieldBuilders.add(new ParentIdFieldMapper.Builder(parentIdFieldName, parent, children));
            return this;
        }

        @Override
        public ParentJoinFieldMapper build(BuilderContext context) {
            checkPreConditions(context.indexCreatedVersion(), context.path(), name);
            fieldType.setName(name);
            final List<ParentIdFieldMapper> parentIdFields = new ArrayList<>();
            parentIdFieldBuilders.stream().map((e) -> e.build(context)).forEach(parentIdFields::add);
            checkParentFields(name(), parentIdFields);
            return new ParentJoinFieldMapper(name, fieldType, context.indexSettings(),
                Collections.unmodifiableList(parentIdFields));
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?,?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final IndexSettings indexSettings = parserContext.mapperService().getIndexSettings();
            if (indexSettings.getIndexMetaData().isRoutingPartitionedIndex()) {
                throw new IllegalStateException("cannot set join field [" + name + "] for the partitioned index " +
                    "[" + indexSettings.getIndex().getName() + "]");
            }

            Builder builder = new Builder(name);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                if ("type".equals(entry.getKey())) {
                    continue;
                }

                final String parent = entry.getKey();
                Set<String> children;
                if (entry.getValue() instanceof List) {
                    children = new HashSet<>();
                    for (Object childObj : (List) entry.getValue()) {
                        if (childObj instanceof String) {
                           children.add(childObj.toString());
                        } else {
                            throw new MapperParsingException("[" + parent + "] expected an array of strings but was:" +
                                childObj.getClass().getSimpleName());
                        }
                    }
                    children = Collections.unmodifiableSet(children);
                } else if (entry.getValue() instanceof String) {
                    children = Collections.singleton(entry.getValue().toString());
                } else {
                    throw new MapperParsingException("[" + parent + "] expected string but was:" +
                        entry.getValue().getClass().getSimpleName());
                }
                builder.addParent(parent, children);
                iterator.remove();
            }
            return builder;
        }
    }

    public static final class JoinFieldType extends StringFieldType {
        public JoinFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected JoinFieldType(JoinFieldType ref) {
            super(ref);
        }

        public JoinFieldType clone() {
            return new JoinFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
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

    private List<ParentIdFieldMapper> parentIdFields;

    protected ParentJoinFieldMapper(String simpleName,
                                    MappedFieldType fieldType,
                                    Settings indexSettings,
                                    List<ParentIdFieldMapper> parentIdFields) {
        super(simpleName, fieldType, Defaults.FIELD_TYPE, indexSettings, MultiFields.empty(), null);
        this.parentIdFields = parentIdFields;
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
        return parentIdFields.stream().map((field) -> (Mapper) field).iterator();
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

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        ParentJoinFieldMapper joinMergeWith = (ParentJoinFieldMapper) mergeWith;
        List<String> conflicts = new ArrayList<>();
        for (ParentIdFieldMapper mapper : parentIdFields) {
            if (joinMergeWith.getParentIdFieldMapper(mapper.getParentName(), true) == null) {
                conflicts.add("cannot remove parent [" + mapper.getParentName() + "] in join field [" + name() + "]");
            }
        }

        final List<ParentIdFieldMapper> newParentIdFields = new ArrayList<>();
        for (ParentIdFieldMapper mergeWithMapper : joinMergeWith.parentIdFields) {
            ParentIdFieldMapper self = getParentIdFieldMapper(mergeWithMapper.getParentName(), true);
            if (self == null) {
                if (getParentIdFieldMapper(mergeWithMapper.getParentName(), false) != null) {
                    // it is forbidden to add a parent to an existing child
                    conflicts.add("cannot create parent [" + mergeWithMapper.getParentName()  + "] from an existing child");
                }
                for (String child : mergeWithMapper.getChildren()) {
                    if (getParentIdFieldMapper(child, true) != null) {
                        // it is forbidden to add a parent to an existing child
                        conflicts.add("cannot create child [" + child  + "] from an existing parent");
                    }
                }
                newParentIdFields.add(mergeWithMapper);
            } else {
                for (String child : self.getChildren()) {
                    if (mergeWithMapper.getChildren().contains(child) == false) {
                        conflicts.add("cannot remove child [" + child + "] in join field [" + name() + "]");
                    }
                }
                ParentIdFieldMapper merged = (ParentIdFieldMapper) self.merge(mergeWithMapper, false);
                newParentIdFields.add(merged);
            }
        }
        if (conflicts.isEmpty() == false) {
            throw new IllegalStateException("invalid update for join field [" + name() + "]:\n" + conflicts.toString());
        }
        this.parentIdFields = Collections.unmodifiableList(newParentIdFields);
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        ParentJoinFieldMapper fieldMapper = (ParentJoinFieldMapper) super.updateFieldType(fullNameToFieldType);
        final List<ParentIdFieldMapper> newMappers = new ArrayList<> ();
        for (ParentIdFieldMapper mapper : fieldMapper.parentIdFields) {
            newMappers.add((ParentIdFieldMapper) mapper.updateFieldType(fullNameToFieldType));
        }
        fieldMapper.parentIdFields = Collections.unmodifiableList(newMappers);
        return fieldMapper;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        // Only one join field per document
        checkDuplicateJoinFields(context.doc());

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
        Field field = new Field(fieldType().name(), binaryValue, fieldType());
        context.doc().add(field);
        context.doc().add(new SortedDocValuesField(fieldType().name(), binaryValue));
        context.path().remove();
        return null;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        for (ParentIdFieldMapper field : parentIdFields) {
            if (field.getChildren().size() == 1) {
                builder.field(field.getParentName(), field.getChildren().iterator().next());
            } else {
                builder.field(field.getParentName(), field.getChildren());
            }
        }
    }
}
