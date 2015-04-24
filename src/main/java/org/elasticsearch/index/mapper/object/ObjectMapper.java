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

package org.elasticsearch.index.mapper.object;

import com.google.common.collect.Iterables;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.CopyOnWriteHashMap;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilders;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperUtils;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.StrictDynamicMappingException;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.index.mapper.MapperBuilders.binaryField;
import static org.elasticsearch.index.mapper.MapperBuilders.booleanField;
import static org.elasticsearch.index.mapper.MapperBuilders.dateField;
import static org.elasticsearch.index.mapper.MapperBuilders.doubleField;
import static org.elasticsearch.index.mapper.MapperBuilders.floatField;
import static org.elasticsearch.index.mapper.MapperBuilders.integerField;
import static org.elasticsearch.index.mapper.MapperBuilders.longField;
import static org.elasticsearch.index.mapper.MapperBuilders.object;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

/**
 *
 */
public class ObjectMapper implements Mapper, AllFieldMapper.IncludeInAll, Cloneable {

    public static final String CONTENT_TYPE = "object";
    public static final String NESTED_CONTENT_TYPE = "nested";

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final Nested NESTED = Nested.NO;
        public static final Dynamic DYNAMIC = null; // not set, inherited from root
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static enum Dynamic {
        TRUE,
        FALSE,
        STRICT
    }

    public static class Nested {

        public static final Nested NO = new Nested(false, false, false);

        public static Nested newNested(boolean includeInParent, boolean includeInRoot) {
            return new Nested(true, includeInParent, includeInRoot);
        }

        private final boolean nested;

        private final boolean includeInParent;

        private final boolean includeInRoot;

        private Nested(boolean nested, boolean includeInParent, boolean includeInRoot) {
            this.nested = nested;
            this.includeInParent = includeInParent;
            this.includeInRoot = includeInRoot;
        }

        public boolean isNested() {
            return nested;
        }

        public boolean isIncludeInParent() {
            return includeInParent;
        }

        public boolean isIncludeInRoot() {
            return includeInRoot;
        }
    }

    public static class Builder<T extends Builder, Y extends ObjectMapper> extends Mapper.Builder<T, Y> {

        protected boolean enabled = Defaults.ENABLED;

        protected Nested nested = Defaults.NESTED;

        protected Dynamic dynamic = Defaults.DYNAMIC;

        protected ContentPath.Type pathType = Defaults.PATH_TYPE;

        protected Boolean includeInAll;

        protected final List<Mapper.Builder> mappersBuilders = newArrayList();

        public Builder(String name) {
            super(name);
            this.builder = (T) this;
        }

        public T enabled(boolean enabled) {
            this.enabled = enabled;
            return builder;
        }

        public T dynamic(Dynamic dynamic) {
            this.dynamic = dynamic;
            return builder;
        }

        public T nested(Nested nested) {
            this.nested = nested;
            return builder;
        }

        public T pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return builder;
        }

        public T includeInAll(boolean includeInAll) {
            this.includeInAll = includeInAll;
            return builder;
        }

        public T add(Mapper.Builder builder) {
            mappersBuilders.add(builder);
            return this.builder;
        }

        @Override
        public Y build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);
            context.path().add(name);

            Map<String, Mapper> mappers = new HashMap<>();
            for (Mapper.Builder builder : mappersBuilders) {
                Mapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            context.path().pathType(origPathType);
            context.path().remove();

            ObjectMapper objectMapper = createMapper(name, context.path().fullPathAsText(name), enabled, nested, dynamic, pathType, mappers, context.indexSettings());
            objectMapper.includeInAllIfNotSet(includeInAll);

            return (Y) objectMapper;
        }

        protected ObjectMapper createMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic, ContentPath.Type pathType, Map<String, Mapper> mappers, @Nullable @IndexSettings Settings settings) {
            return new ObjectMapper(name, fullPath, enabled, nested, dynamic, pathType, mappers);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ObjectMapper.Builder builder = createBuilder(name);
            parseNested(name, node, builder);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (parseObjectOrDocumentTypeProperties(fieldName, fieldNode, parserContext, builder) || parseObjectProperties(name, fieldName,  fieldNode, parserContext, builder)) {
                    iterator.remove();
                }
            }
            return builder;
        }

        protected static boolean parseObjectOrDocumentTypeProperties(String fieldName, Object fieldNode, ParserContext parserContext, ObjectMapper.Builder builder) {
            if (fieldName.equals("dynamic")) {
                String value = fieldNode.toString();
                if (value.equalsIgnoreCase("strict")) {
                    builder.dynamic(Dynamic.STRICT);
                } else {
                    builder.dynamic(nodeBooleanValue(fieldNode) ? Dynamic.TRUE : Dynamic.FALSE);
                }
                return true;
            } else if (fieldName.equals("enabled")) {
                builder.enabled(nodeBooleanValue(fieldNode));
                return true;
            } else if (fieldName.equals("properties")) {
                if (fieldNode instanceof Collection && ((Collection) fieldNode).isEmpty()) {
                    // nothing to do here, empty (to support "properties: []" case)
                } else if (!(fieldNode instanceof Map)) {
                    throw new ElasticsearchParseException("properties must be a map type");
                } else {
                    parseProperties(builder, (Map<String, Object>) fieldNode, parserContext);
                }
                return true;
            } else if (fieldName.equals("include_in_all")) {
                builder.includeInAll(nodeBooleanValue(fieldNode));
                return true;
            }
            return false;
        }

        protected static boolean parseObjectProperties(String name, String fieldName, Object fieldNode, ParserContext parserContext, ObjectMapper.Builder builder) {
            if (fieldName.equals("path") && parserContext.indexVersionCreated().before(Version.V_2_0_0)) {
                builder.pathType(parsePathType(name, fieldNode.toString()));
                return true;
            }
           return false;
        }

        protected static void parseNested(String name, Map<String, Object> node, ObjectMapper.Builder builder) {
            boolean nested = false;
            boolean nestedIncludeInParent = false;
            boolean nestedIncludeInRoot = false;
            Object fieldNode = node.get("type");
            if (fieldNode!=null) {
                String type = fieldNode.toString();
                if (type.equals(CONTENT_TYPE)) {
                    builder.nested = Nested.NO;
                } else if (type.equals(NESTED_CONTENT_TYPE)) {
                    nested = true;
                } else {
                    throw new MapperParsingException("Trying to parse an object but has a different type [" + type + "] for [" + name + "]");
                }
            }
            fieldNode = node.get("include_in_parent");
            if (fieldNode != null) {
                nestedIncludeInParent = nodeBooleanValue(fieldNode);
                node.remove("include_in_parent");
            }
            fieldNode = node.get("include_in_root");
            if (fieldNode != null) {
                nestedIncludeInRoot = nodeBooleanValue(fieldNode);
                node.remove("include_in_root");
            }
            if (nested) {
                builder.nested = Nested.newNested(nestedIncludeInParent, nestedIncludeInRoot);
            }

        }

        protected static void parseProperties(ObjectMapper.Builder objBuilder, Map<String, Object> propsNode, ParserContext parserContext) {
            Iterator<Map.Entry<String, Object>> iterator = propsNode.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                // Should accept empty arrays, as a work around for when the
                // user can't provide an empty Map. (PHP for example)
                boolean isEmptyList = entry.getValue() instanceof List && ((List<?>) entry.getValue()).isEmpty();

                if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                    String type;
                    Object typeNode = propNode.get("type");
                    if (typeNode != null) {
                        type = typeNode.toString();
                    } else {
                        // lets see if we can derive this...
                        if (propNode.get("properties") != null) {
                            type = ObjectMapper.CONTENT_TYPE;
                        } else if (propNode.size() == 1 && propNode.get("enabled") != null) {
                            // if there is a single property with the enabled
                            // flag on it, make it an object
                            // (usually, setting enabled to false to not index
                            // any type, including core values, which
                            type = ObjectMapper.CONTENT_TYPE;
                        } else {
                            throw new MapperParsingException("No type specified for property [" + propName + "]");
                        }
                    }

                    Mapper.TypeParser typeParser = parserContext.typeParser(type);
                    if (typeParser == null) {
                        throw new MapperParsingException("No handler for type [" + type + "] declared on field [" + propName + "]");
                    }
                    objBuilder.add(typeParser.parse(propName, propNode, parserContext));
                    propNode.remove("type");
                    DocumentMapperParser.checkNoRemainingFields(propName, propNode, parserContext.indexVersionCreated());
                    iterator.remove();
                } else if (isEmptyList) {
                    iterator.remove();
                } else {
                    throw new MapperParsingException("Expected map for property [fields] on field [" + propName + "] but got a "
                            + propName.getClass());
                }
            }

            DocumentMapperParser.checkNoRemainingFields(propsNode, parserContext.indexVersionCreated(),
                    "DocType mapping definition has unsupported parameters: ");

        }

        protected Builder createBuilder(String name) {
            return object(name);
        }
    }

    private final String name;

    private final String fullPath;

    private final boolean enabled;

    private final Nested nested;

    private final String nestedTypePathAsString;
    private final BytesRef nestedTypePathAsBytes;

    private final Filter nestedTypeFilter;

    private volatile Dynamic dynamic;

    private final ContentPath.Type pathType;

    private Boolean includeInAll;

    private volatile CopyOnWriteHashMap<String, Mapper> mappers;

    ObjectMapper(String name, String fullPath, boolean enabled, Nested nested, Dynamic dynamic, ContentPath.Type pathType, Map<String, Mapper> mappers) {
        this.name = name;
        this.fullPath = fullPath;
        this.enabled = enabled;
        this.nested = nested;
        this.dynamic = dynamic;
        this.pathType = pathType;
        if (mappers == null) {
            this.mappers = new CopyOnWriteHashMap<>();
        } else {
            this.mappers = CopyOnWriteHashMap.copyOf(mappers);
        }
        this.nestedTypePathAsString = "__" + fullPath;
        this.nestedTypePathAsBytes = new BytesRef(nestedTypePathAsString);
        this.nestedTypeFilter = Queries.wrap(new TermQuery(new Term(TypeFieldMapper.NAME, nestedTypePathAsBytes)));
    }

    @Override
    protected ObjectMapper clone() {
        ObjectMapper clone;
        try {
            clone = (ObjectMapper) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException();
        }
        return clone;
    }

    /**
     * Build a mapping update with the provided sub mapping update.
     */
    public ObjectMapper mappingUpdate(Mapper mapper) {
        ObjectMapper mappingUpdate = clone();
        // reset the sub mappers
        mappingUpdate.mappers = new CopyOnWriteHashMap<>();
        mappingUpdate.putMapper(mapper);
        return mappingUpdate;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll == null) {
            return;
        }
        this.includeInAll = includeInAll;
        // when called from outside, apply this on all the inner mappers
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof AllFieldMapper.IncludeInAll) {
                ((AllFieldMapper.IncludeInAll) mapper).includeInAll(includeInAll);
            }
        }
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
        // when called from outside, apply this on all the inner mappers
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof AllFieldMapper.IncludeInAll) {
                ((AllFieldMapper.IncludeInAll) mapper).includeInAllIfNotSet(includeInAll);
            }
        }
    }

    @Override
    public void unsetIncludeInAll() {
        includeInAll = null;
        // when called from outside, apply this on all the inner mappers
        for (Mapper mapper : mappers.values()) {
            if (mapper instanceof AllFieldMapper.IncludeInAll) {
                ((AllFieldMapper.IncludeInAll) mapper).unsetIncludeInAll();
            }
        }
    }

    public Nested nested() {
        return this.nested;
    }

    public Filter nestedTypeFilter() {
        return this.nestedTypeFilter;
    }

    /**
     * Put a new mapper.
     * NOTE: this method must be called under the current {@link DocumentMapper}
     * lock if concurrent updates are expected.
     */
    public void putMapper(Mapper mapper) {
        if (mapper instanceof AllFieldMapper.IncludeInAll) {
            ((AllFieldMapper.IncludeInAll) mapper).includeInAllIfNotSet(includeInAll);
        }
        mappers = mappers.copyAndPut(mapper.name(), mapper);
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        for (Mapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
        objectMapperListener.objectMapper(this);
        for (Mapper mapper : mappers.values()) {
            mapper.traverse(objectMapperListener);
        }
    }

    public String fullPath() {
        return this.fullPath;
    }

    public String nestedTypePathAsString() {
        return nestedTypePathAsString;
    }

    public final Dynamic dynamic() {
        return this.dynamic == null ? Dynamic.TRUE : this.dynamic;
    }

    public void setDynamic(Dynamic dynamic) {
        this.dynamic = dynamic;
    }

    protected boolean allowValue() {
        return true;
    }

    @Override
    public ObjectMapper parse(ParseContext context) throws IOException {
        if (!enabled) {
            context.parser().skipChildren();
            return null;
        }
        XContentParser parser = context.parser();

        String currentFieldName = parser.currentName();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            // the object is null ("obj1" : null), simply bail
            return null;
        }

        if (token.isValue() && !allowValue()) {
            // if we are parsing an object but it is just a value, its only allowed on root level parsers with there
            // is a field name with the same name as the type
            throw new MapperParsingException("object mapping for [" + name + "] tried to parse field [" + currentFieldName + "] as object, but found a concrete value");
        }

        if (nested.isNested()) {
            context = context.createNestedContext(fullPath);
            Document nestedDoc = context.doc();
            Document parentDoc = nestedDoc.getParent();
            // pre add the uid field if possible (id was already provided)
            IndexableField uidField = parentDoc.getField(UidFieldMapper.NAME);
            if (uidField != null) {
                // we don't need to add it as a full uid field in nested docs, since we don't need versioning
                // we also rely on this for UidField#loadVersion

                // this is a deeply nested field
                nestedDoc.add(new Field(UidFieldMapper.NAME, uidField.stringValue(), UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
            }
            // the type of the nested doc starts with __, so we can identify that its a nested one in filters
            // note, we don't prefix it with the type of the doc since it allows us to execute a nested query
            // across types (for example, with similar nested objects)
            nestedDoc.add(new Field(TypeFieldMapper.NAME, nestedTypePathAsString, TypeFieldMapper.Defaults.FIELD_TYPE));
        }

        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);

        // if we are at the end of the previous object, advance
        if (token == XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            // if we are just starting an OBJECT, advance, this is the object we are parsing, we need the name first
            token = parser.nextToken();
        }

        ObjectMapper update = null;
        while (token != XContentParser.Token.END_OBJECT) {
            ObjectMapper newUpdate = null;
            if (token == XContentParser.Token.START_OBJECT) {
                newUpdate = serializeObject(context, currentFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                newUpdate = serializeArray(context, currentFieldName);
            } else if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                serializeNullValue(context, currentFieldName);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + name + "] tried to parse field [" + currentFieldName + "] as object, but got EOF, has a concrete value been provided to it?");
            } else if (token.isValue()) {
                newUpdate = serializeValue(context, currentFieldName, token);
            }
            token = parser.nextToken();
            if (newUpdate != null) {
                if (update == null) {
                    update = newUpdate;
                } else {
                    MapperUtils.merge(update, newUpdate);
                }
            }
        }
        // restore the enable path flag
        context.path().pathType(origPathType);
        if (nested.isNested()) {
            Document nestedDoc = context.doc();
            Document parentDoc = nestedDoc.getParent();
            if (nested.isIncludeInParent()) {
                for (IndexableField field : nestedDoc.getFields()) {
                    if (field.name().equals(UidFieldMapper.NAME) || field.name().equals(TypeFieldMapper.NAME)) {
                        continue;
                    } else {
                        parentDoc.add(field);
                    }
                }
            }
            if (nested.isIncludeInRoot()) {
                Document rootDoc = context.rootDoc();
                // don't add it twice, if its included in parent, and we are handling the master doc...
                if (!nested.isIncludeInParent() || parentDoc != rootDoc) {
                    for (IndexableField field : nestedDoc.getFields()) {
                        if (field.name().equals(UidFieldMapper.NAME) || field.name().equals(TypeFieldMapper.NAME)) {
                            continue;
                        } else {
                            rootDoc.add(field);
                        }
                    }
                }
            }
        }
        return update;
    }

    private void serializeNullValue(ParseContext context, String lastFieldName) throws IOException {
        // we can only handle null values if we have mappings for them
        Mapper mapper = mappers.get(lastFieldName);
        if (mapper != null) {
            if (mapper instanceof FieldMapper) {
                if (!((FieldMapper) mapper).supportsNullValue()) {
                    throw new MapperParsingException("no object mapping found for null value in [" + lastFieldName + "]");
                }
            }
            mapper.parse(context);
        } else if (dynamic == Dynamic.STRICT) {
            throw new StrictDynamicMappingException(fullPath, lastFieldName);
        }
    }

    private ObjectMapper serializeObject(final ParseContext context, String currentFieldName) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + name + "] trying to serialize an object with no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        context.path().add(currentFieldName);

        ObjectMapper update = null;
        Mapper objectMapper = mappers.get(currentFieldName);
        if (objectMapper != null) {
            final Mapper subUpdate = objectMapper.parse(context);
            if (subUpdate != null) {
                // propagate mapping update
                update = mappingUpdate(subUpdate);
            }
        } else {
            Dynamic dynamic = this.dynamic;
            if (dynamic == null) {
                dynamic = context.root().dynamic();
            }
            if (dynamic == Dynamic.STRICT) {
                throw new StrictDynamicMappingException(fullPath, currentFieldName);
            } else if (dynamic == Dynamic.TRUE) {
                // remove the current field name from path, since template search and the object builder add it as well...
                context.path().remove();
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "object");
                if (builder == null) {
                    builder = MapperBuilders.object(currentFieldName).enabled(true).pathType(pathType);
                    // if this is a non root object, then explicitly set the dynamic behavior if set
                    if (!(this instanceof RootObjectMapper) && this.dynamic != Defaults.DYNAMIC) {
                        ((Builder) builder).dynamic(this.dynamic);
                    }
                }
                BuilderContext builderContext = new BuilderContext(context.indexSettings(), context.path());
                objectMapper = builder.build(builderContext);
                context.path().add(currentFieldName);
                update = mappingUpdate(MapperUtils.parseAndMergeUpdate(objectMapper, context));
            } else {
                // not dynamic, read everything up to end object
                context.parser().skipChildren();
            }
        }

        context.path().remove();
        return update;
    }

    private ObjectMapper serializeArray(ParseContext context, String lastFieldName) throws IOException {
        String arrayFieldName = lastFieldName;
        Mapper mapper = mappers.get(lastFieldName);
        if (mapper != null) {
            // There is a concrete mapper for this field already. Need to check if the mapper 
            // expects an array, if so we pass the context straight to the mapper and if not 
            // we serialize the array components
            if (mapper instanceof ArrayValueMapperParser) {
                final Mapper subUpdate = mapper.parse(context);
                if (subUpdate != null) {
                    // propagate the mapping update
                    return mappingUpdate(subUpdate);
                } else {
                    return null;
                }
            } else {
                return serializeNonDynamicArray(context, lastFieldName, arrayFieldName);
            }
        } else {

            Dynamic dynamic = this.dynamic;
            if (dynamic == null) {
                dynamic = context.root().dynamic();
            }
            if (dynamic == Dynamic.STRICT) {
                throw new StrictDynamicMappingException(fullPath, arrayFieldName);
            } else if (dynamic == Dynamic.TRUE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, arrayFieldName, "object");
                if (builder == null) {
                    return serializeNonDynamicArray(context, lastFieldName, arrayFieldName);
                }
                BuilderContext builderContext = new BuilderContext(context.indexSettings(), context.path());
                mapper = builder.build(builderContext);
                if (mapper != null && mapper instanceof ArrayValueMapperParser) {
                    context.path().add(arrayFieldName);
                    mapper = MapperUtils.parseAndMergeUpdate(mapper, context);
                    return mappingUpdate(mapper);
                } else {
                    return serializeNonDynamicArray(context, lastFieldName, arrayFieldName);
                }
            } else {
                return serializeNonDynamicArray(context, lastFieldName, arrayFieldName);
            }
        }
    }

    private ObjectMapper serializeNonDynamicArray(ParseContext context, String lastFieldName, String arrayFieldName) throws IOException {
        XContentParser parser = context.parser();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                return serializeObject(context, lastFieldName);
            } else if (token == XContentParser.Token.START_ARRAY) {
                return serializeArray(context, lastFieldName);
            } else if (token == XContentParser.Token.FIELD_NAME) {
                lastFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NULL) {
                serializeNullValue(context, lastFieldName);
            } else if (token == null) {
                throw new MapperParsingException("object mapping for [" + name + "] with array for [" + arrayFieldName + "] tried to parse as array, but got EOF, is there a mismatch in types for the same field?");
            } else {
                return serializeValue(context, lastFieldName, token);
            }
        }
        return null;
    }

    private ObjectMapper serializeValue(final ParseContext context, String currentFieldName, XContentParser.Token token) throws IOException {
        if (currentFieldName == null) {
            throw new MapperParsingException("object mapping [" + name + "] trying to serialize a value with no field associated with it, current value [" + context.parser().textOrNull() + "]");
        }
        Mapper mapper = mappers.get(currentFieldName);
        if (mapper != null) {
            Mapper subUpdate = mapper.parse(context);
            if (subUpdate == null) {
                return null;
            }
            return mappingUpdate(subUpdate);
        } else {
            return parseDynamicValue(context, currentFieldName, token);
        }
    }

    public ObjectMapper parseDynamicValue(final ParseContext context, String currentFieldName, XContentParser.Token token) throws IOException {
        Dynamic dynamic = this.dynamic;
        if (dynamic == null) {
            dynamic = context.root().dynamic();
        }
        if (dynamic == Dynamic.STRICT) {
            throw new StrictDynamicMappingException(fullPath, currentFieldName);
        }
        if (dynamic == Dynamic.FALSE) {
            return null;
        }
        Mapper mapper = null;
        BuilderContext builderContext = new BuilderContext(context.indexSettings(), context.path());
        if (token == XContentParser.Token.VALUE_STRING) {
            boolean resolved = false;

            // do a quick test to see if its fits a dynamic template, if so, use it.
            // we need to do it here so we can handle things like attachment templates, where calling
            // text (to see if its a date) causes the binary value to be cleared
            if (!resolved) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string", null);
                if (builder != null) {
                    mapper = builder.build(builderContext);
                    resolved = true;
                }
            }

            if (!resolved && context.root().dateDetection()) {
                String text = context.parser().text();
                // a safe check since "1" gets parsed as well
                if (Strings.countOccurrencesOf(text, ":") > 1 || Strings.countOccurrencesOf(text, "-") > 1 || Strings.countOccurrencesOf(text, "/") > 1) {
                    for (FormatDateTimeFormatter dateTimeFormatter : context.root().dynamicDateTimeFormatters()) {
                        try {
                            dateTimeFormatter.parser().parseMillis(text);
                            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "date");
                            if (builder == null) {
                                builder = dateField(currentFieldName).dateTimeFormatter(dateTimeFormatter);
                            }
                            mapper = builder.build(builderContext);
                            resolved = true;
                            break;
                        } catch (Exception e) {
                            // failure to parse this, continue
                        }
                    }
                }
            }
            if (!resolved && context.root().numericDetection()) {
                String text = context.parser().text();
                try {
                    Long.parseLong(text);
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = longField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                    resolved = true;
                } catch (Exception e) {
                    // not a long number
                }
                if (!resolved) {
                    try {
                        Double.parseDouble(text);
                        Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                        if (builder == null) {
                            builder = doubleField(currentFieldName);
                        }
                        mapper = builder.build(builderContext);
                        resolved = true;
                    } catch (Exception e) {
                        // not a long number
                    }
                }
            }
            if (!resolved) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "string");
                if (builder == null) {
                    builder = stringField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            }
        } else if (token == XContentParser.Token.VALUE_NUMBER) {
            XContentParser.NumberType numberType = context.parser().numberType();
            if (numberType == XContentParser.NumberType.INT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                    if (builder == null) {
                        builder = longField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "integer");
                    if (builder == null) {
                        builder = integerField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (numberType == XContentParser.NumberType.LONG) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "long");
                if (builder == null) {
                    builder = longField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            } else if (numberType == XContentParser.NumberType.FLOAT) {
                if (context.parser().estimatedNumberType()) {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                    if (builder == null) {
                        builder = doubleField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                } else {
                    Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "float");
                    if (builder == null) {
                        builder = floatField(currentFieldName);
                    }
                    mapper = builder.build(builderContext);
                }
            } else if (numberType == XContentParser.NumberType.DOUBLE) {
                Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "double");
                if (builder == null) {
                    builder = doubleField(currentFieldName);
                }
                mapper = builder.build(builderContext);
            }
        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "boolean");
            if (builder == null) {
                builder = booleanField(currentFieldName);
            }
            mapper = builder.build(builderContext);
        } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, "binary");
            if (builder == null) {
                builder = binaryField(currentFieldName);
            }
            mapper = builder.build(builderContext);
        } else {
            Mapper.Builder builder = context.root().findTemplateBuilder(context, currentFieldName, null);
            if (builder != null) {
                mapper = builder.build(builderContext);
            } else {
                // TODO how do we identify dynamically that its a binary value?
                throw new ElasticsearchIllegalStateException("Can't handle serializing a dynamic type with content token [" + token + "] and field name [" + currentFieldName + "]");
            }
        }

        mapper = MapperUtils.parseAndMergeUpdate(mapper, context);

        ObjectMapper update = null;
        if (mapper != null) {
            update = mappingUpdate(mapper);
        }
        return update;
    }

    @Override
    public void merge(final Mapper mergeWith, final MergeResult mergeResult) throws MergeMappingException {
        if (!(mergeWith instanceof ObjectMapper)) {
            mergeResult.addConflict("Can't merge a non object mapping [" + mergeWith.name() + "] with an object mapping [" + name() + "]");
            return;
        }
        ObjectMapper mergeWithObject = (ObjectMapper) mergeWith;

        if (nested().isNested()) {
            if (!mergeWithObject.nested().isNested()) {
                mergeResult.addConflict("object mapping [" + name() + "] can't be changed from nested to non-nested");
                return;
            }
        } else {
            if (mergeWithObject.nested().isNested()) {
                mergeResult.addConflict("object mapping [" + name() + "] can't be changed from non-nested to nested");
                return;
            }
        }

        if (!mergeResult.simulate()) {
            if (mergeWithObject.dynamic != null) {
                this.dynamic = mergeWithObject.dynamic;
            }
        }

        doMerge(mergeWithObject, mergeResult);

        List<Mapper> mappersToPut = new ArrayList<>();
        FieldMapperListener.Aggregator newFieldMappers = new FieldMapperListener.Aggregator();
        ObjectMapperListener.Aggregator newObjectMappers = new ObjectMapperListener.Aggregator();
        for (Mapper mapper : mergeWithObject.mappers.values()) {
            Mapper mergeWithMapper = mapper;
            Mapper mergeIntoMapper = mappers.get(mergeWithMapper.name());
            if (mergeIntoMapper == null) {
                // no mapping, simply add it if not simulating
                if (!mergeResult.simulate()) {
                    mappersToPut.add(mergeWithMapper);
                    mergeWithMapper.traverse(newFieldMappers);
                    mergeWithMapper.traverse(newObjectMappers);
                }
            } else {
                mergeIntoMapper.merge(mergeWithMapper, mergeResult);
            }
        }
        if (!newFieldMappers.mappers.isEmpty()) {
            mergeResult.addFieldMappers(newFieldMappers.mappers);
        }
        if (!newObjectMappers.mappers.isEmpty()) {
            mergeResult.addObjectMappers(newObjectMappers.mappers);
        }
        // add the mappers only after the administration have been done, so it will not be visible to parser (which first try to read with no lock)
        for (Mapper mapper : mappersToPut) {
            putMapper(mapper);
        }
    }

    protected void doMerge(ObjectMapper mergeWith, MergeResult mergeResult) {

    }

    @Override
    public void close() {
        for (Mapper mapper : mappers.values()) {
            mapper.close();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        toXContent(builder, params, null, Mapper.EMPTY_ARRAY);
        return builder;
    }

    public void toXContent(XContentBuilder builder, Params params, ToXContent custom, Mapper... additionalMappers) throws IOException {
        builder.startObject(name);
        if (nested.isNested()) {
            builder.field("type", NESTED_CONTENT_TYPE);
            if (nested.isIncludeInParent()) {
                builder.field("include_in_parent", true);
            }
            if (nested.isIncludeInRoot()) {
                builder.field("include_in_root", true);
            }
        } else if (mappers.isEmpty()) { // only write the object content type if there are no properties, otherwise, it is automatically detected
            builder.field("type", CONTENT_TYPE);
        }
        if (dynamic != null) {
            builder.field("dynamic", dynamic.name().toLowerCase(Locale.ROOT));
        }
        if (enabled != Defaults.ENABLED) {
            builder.field("enabled", enabled);
        }
        if (pathType != Defaults.PATH_TYPE) {
            builder.field("path", pathType.name().toLowerCase(Locale.ROOT));
        }
        if (includeInAll != null) {
            builder.field("include_in_all", includeInAll);
        }

        if (custom != null) {
            custom.toXContent(builder, params);
        }

        doXContent(builder, params);

        // sort the mappers so we get consistent serialization format
        Mapper[] sortedMappers = Iterables.toArray(mappers.values(), Mapper.class);
        Arrays.sort(sortedMappers, new Comparator<Mapper>() {
            @Override
            public int compare(Mapper o1, Mapper o2) {
                return o1.name().compareTo(o2.name());
            }
        });

        // check internal mappers first (this is only relevant for root object)
        for (Mapper mapper : sortedMappers) {
            if (mapper instanceof InternalMapper) {
                mapper.toXContent(builder, params);
            }
        }
        if (additionalMappers != null && additionalMappers.length > 0) {
            TreeMap<String, Mapper> additionalSortedMappers = new TreeMap<>();
            for (Mapper mapper : additionalMappers) {
                additionalSortedMappers.put(mapper.name(), mapper);
            }

            for (Mapper mapper : additionalSortedMappers.values()) {
                mapper.toXContent(builder, params);
            }
        }

        int count = 0;
        for (Mapper mapper : sortedMappers) {
            if (!(mapper instanceof InternalMapper)) {
                if (count++ == 0) {
                    builder.startObject("properties");
                }
                mapper.toXContent(builder, params);
            }
        }
        if (count > 0) {
            builder.endObject();
        }
        builder.endObject();
    }

    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

    }

}
