/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.json;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.util.Nullable;
import org.elasticsearch.util.Preconditions;
import org.elasticsearch.util.io.FastStringReader;
import org.elasticsearch.util.json.Jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JsonDocumentMapper implements DocumentMapper {

    public static class Builder {

        private JsonUidFieldMapper uidFieldMapper = new JsonUidFieldMapper();

        private JsonIdFieldMapper idFieldMapper = new JsonIdFieldMapper();

        private JsonTypeFieldMapper typeFieldMapper = new JsonTypeFieldMapper();

        private JsonSourceFieldMapper sourceFieldMapper = new JsonSourceFieldMapper();

        private JsonBoostFieldMapper boostFieldMapper = new JsonBoostFieldMapper();

        private Analyzer indexAnalyzer;

        private Analyzer searchAnalyzer;

        private final JsonObjectMapper rootObjectMapper;

        private String mappingSource;

        private JsonMapper.BuilderContext builderContext = new JsonMapper.BuilderContext(new JsonPath(1));

        public Builder(JsonObjectMapper.Builder builder) {
            this.rootObjectMapper = builder.build(builderContext);
        }

        public Builder sourceField(JsonSourceFieldMapper.Builder builder) {
            this.sourceFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder idField(JsonIdFieldMapper.Builder builder) {
            this.idFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder uidField(JsonUidFieldMapper.Builder builder) {
            this.uidFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder typeField(JsonTypeFieldMapper.Builder builder) {
            this.typeFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder boostField(JsonBoostFieldMapper.Builder builder) {
            this.boostFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder mappingSource(String mappingSource) {
            this.mappingSource = mappingSource;
            return this;
        }

        public Builder indexAnalyzer(Analyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public boolean hasIndexAnalyzer() {
            return indexAnalyzer != null;
        }

        public Builder searchAnalyzer(Analyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public boolean hasSearchAnalyzer() {
            return searchAnalyzer != null;
        }

        public JsonDocumentMapper build() {
            Preconditions.checkNotNull(rootObjectMapper, "Json mapper builder must have the root object mapper set");
            return new JsonDocumentMapper(rootObjectMapper, uidFieldMapper, idFieldMapper, typeFieldMapper,
                    sourceFieldMapper, indexAnalyzer, searchAnalyzer, boostFieldMapper, mappingSource);
        }
    }


    private ThreadLocal<JsonParseContext> cache = new ThreadLocal<JsonParseContext>() {
        @Override protected JsonParseContext initialValue() {
            return new JsonParseContext(JsonDocumentMapper.this, new JsonPath(0));
        }
    };

    private final JsonFactory jsonFactory = Jackson.defaultJsonFactory();

    private final String type;

    private final String mappingSource;

    private final JsonUidFieldMapper uidFieldMapper;

    private final JsonIdFieldMapper idFieldMapper;

    private final JsonTypeFieldMapper typeFieldMapper;

    private final JsonSourceFieldMapper sourceFieldMapper;

    private final JsonBoostFieldMapper boostFieldMapper;

    private final JsonObjectMapper rootObjectMapper;

    private final Analyzer indexAnalyzer;

    private final Analyzer searchAnalyzer;

    private volatile DocumentFieldMappers fieldMappers;

    private final List<FieldMapperListener> fieldMapperListeners = newArrayList();

    private final Object mutex = new Object();

    public JsonDocumentMapper(JsonObjectMapper rootObjectMapper,
                              JsonUidFieldMapper uidFieldMapper,
                              JsonIdFieldMapper idFieldMapper,
                              JsonTypeFieldMapper typeFieldMapper,
                              JsonSourceFieldMapper sourceFieldMapper,
                              Analyzer indexAnalyzer, Analyzer searchAnalyzer,
                              @Nullable JsonBoostFieldMapper boostFieldMapper,
                              @Nullable String mappingSource) {
        this.type = rootObjectMapper.name();
        this.mappingSource = mappingSource;
        this.rootObjectMapper = rootObjectMapper;
        this.uidFieldMapper = uidFieldMapper;
        this.idFieldMapper = idFieldMapper;
        this.typeFieldMapper = typeFieldMapper;
        this.sourceFieldMapper = sourceFieldMapper;
        this.boostFieldMapper = boostFieldMapper;

        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;

        rootObjectMapper.putMapper(idFieldMapper);
        if (boostFieldMapper != null) {
            rootObjectMapper.putMapper(boostFieldMapper);
        }

        final List<FieldMapper> tempFieldMappers = new ArrayList<FieldMapper>();
        // add the basic ones
        tempFieldMappers.add(typeFieldMapper);
        tempFieldMappers.add(sourceFieldMapper);
        tempFieldMappers.add(uidFieldMapper);
        if (boostFieldMapper != null) {
            tempFieldMappers.add(boostFieldMapper);
        }
        // now traverse and get all the statically defined ones
        rootObjectMapper.traverse(new FieldMapperListener() {
            @Override public void fieldMapper(FieldMapper fieldMapper) {
                tempFieldMappers.add(fieldMapper);
            }
        });

        this.fieldMappers = new DocumentFieldMappers(this, tempFieldMappers);
    }

    @Override public String type() {
        return this.type;
    }

    @Override public String mappingSource() {
        return this.mappingSource;
    }

    @Override public UidFieldMapper uidMapper() {
        return this.uidFieldMapper;
    }

    @Override public IdFieldMapper idMapper() {
        return this.idFieldMapper;
    }

    @Override public TypeFieldMapper typeMapper() {
        return this.typeFieldMapper;
    }

    @Override public SourceFieldMapper sourceMapper() {
        return this.sourceFieldMapper;
    }

    @Override public BoostFieldMapper boostMapper() {
        return this.boostFieldMapper;
    }

    @Override public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    @Override public ParsedDocument parse(String source) {
        return parse(null, null, source);
    }

    @Override public ParsedDocument parse(String type, String id, String source) {
        JsonParseContext jsonContext = cache.get();

        if (type != null && !type.equals(this.type)) {
            throw new MapperParsingException("Type mismatch, provide type [" + type + "] but mapper is of type [" + this.type + "]");
        }
        type = this.type;

        try {
            JsonParser jp = jsonFactory.createJsonParser(new FastStringReader(source));
            jsonContext.reset(jp, new Document(), type, source);

            // will result in JsonToken.START_OBJECT
            JsonToken token = jp.nextToken();
            if (token != JsonToken.START_OBJECT) {
                throw new MapperException("Malformed json, must start with an object");
            }
            token = jp.nextToken();
            if (token != JsonToken.FIELD_NAME) {
                throw new MapperException("Malformed json, after first object, the type name must exists");
            }
            if (!jp.getCurrentName().equals(type)) {
                if (type == null) {
                    throw new MapperException("Json content type [" + jp.getCurrentName() + "] does not match the type of the mapper [" + type + "]");
                }
                // continue
            } else {
                // now move to the actual content, which is the start object
                token = jp.nextToken();
                if (token != JsonToken.START_OBJECT) {
                    throw new MapperException("Malformed json, after type is must start with an object");
                }
            }

            if (sourceFieldMapper.enabled()) {
                sourceFieldMapper.parse(jsonContext);
            }
            // set the id if we have it so we can validate it later on, also, add the uid if we can
            if (id != null) {
                jsonContext.id(id);
                uidFieldMapper.parse(jsonContext);
            }
            typeFieldMapper.parse(jsonContext);

            rootObjectMapper.parse(jsonContext);

            // if we did not get the id, we need to parse the uid into the document now, after it was added
            if (id == null) {
                uidFieldMapper.parse(jsonContext);
            }
            if (jsonContext.parsedIdState() != JsonParseContext.ParsedIdState.PARSED) {
                // mark it as external, so we can parse it
                jsonContext.parsedId(JsonParseContext.ParsedIdState.EXTERNAL);
                idFieldMapper.parse(jsonContext);
            }
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse", e);
        }
        return new ParsedDocument(jsonContext.uid(), jsonContext.id(), jsonContext.type(), jsonContext.doc(), source);
    }

    void addFieldMapper(FieldMapper fieldMapper) {
        synchronized (mutex) {
            fieldMappers = fieldMappers.concat(this, fieldMapper);
            for (FieldMapperListener listener : fieldMapperListeners) {
                listener.fieldMapper(fieldMapper);
            }
        }
    }

    @Override public void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting) {
        synchronized (mutex) {
            fieldMapperListeners.add(fieldMapperListener);
            if (includeExisting) {
                fieldMapperListener.fieldMapper(sourceFieldMapper);
                fieldMapperListener.fieldMapper(typeFieldMapper);
                fieldMapperListener.fieldMapper(idFieldMapper);
                fieldMapperListener.fieldMapper(uidFieldMapper);
                rootObjectMapper.traverse(fieldMapperListener);
            }
        }
    }
}
