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

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.*;
import static com.google.common.collect.Lists.*;
import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.util.MapBuilder.*;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class JsonObjectMapper implements JsonMapper {

    public static class Defaults {
        public static final boolean ENABLED = true;
        public static final boolean DYNAMIC = true;
        public static final JsonPath.Type PATH_TYPE = JsonPath.Type.FULL;
        public static final DateTimeFormatter[] DATE_TIME_FORMATTERS = new DateTimeFormatter[]{JsonDateFieldMapper.Defaults.DATE_TIME_FORMATTER};
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonObjectMapper> {

        private boolean enabled = Defaults.ENABLED;

        private boolean dynamic = Defaults.DYNAMIC;

        private JsonPath.Type pathType = Defaults.PATH_TYPE;

        private List<DateTimeFormatter> dateTimeFormatters = newArrayList();

        private final List<JsonMapper.Builder> mappersBuilders = newArrayList();

        public Builder(String name) {
            super(name);
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder dynamic(boolean dynamic) {
            this.dynamic = dynamic;
            return this;
        }

        public Builder pathType(JsonPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder noDateTimeFormatter() {
            this.dateTimeFormatters = null;
            return this;
        }

        public Builder dateTimeFormatter(Iterable<DateTimeFormatter> dateTimeFormatters) {
            for (DateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                this.dateTimeFormatters.add(dateTimeFormatter);
            }
            return this;
        }

        public Builder dateTimeFormatter(DateTimeFormatter[] dateTimeFormatters) {
            this.dateTimeFormatters.addAll(newArrayList(dateTimeFormatters));
            return this;
        }

        public Builder dateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
            this.dateTimeFormatters.add(dateTimeFormatter);
            return this;
        }

        public Builder add(JsonMapper.Builder builder) {
            mappersBuilders.add(builder);
            return this;
        }

        @Override public JsonObjectMapper build(BuilderContext context) {
            if (dateTimeFormatters == null) {
                dateTimeFormatters = newArrayList();
            } else if (dateTimeFormatters.isEmpty()) {
                // add the default one
                dateTimeFormatters.addAll(newArrayList(Defaults.DATE_TIME_FORMATTERS));
            }
            JsonPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);
            context.path().add(name);

            Map<String, JsonMapper> mappers = new HashMap<String, JsonMapper>();
            for (JsonMapper.Builder builder : mappersBuilders) {
                JsonMapper mapper = builder.build(context);
                mappers.put(mapper.name(), mapper);
            }
            JsonObjectMapper objectMapper = new JsonObjectMapper(name, enabled, dynamic, pathType,
                    dateTimeFormatters.toArray(new DateTimeFormatter[dateTimeFormatters.size()]),
                    mappers);

            context.path().pathType(origPathType);
            context.path().remove();

            return objectMapper;
        }
    }

    private final String name;

    private final boolean enabled;

    private final boolean dynamic;

    private final JsonPath.Type pathType;

    private final DateTimeFormatter[] dateTimeFormatters;

    private volatile ImmutableMap<String, JsonMapper> mappers = ImmutableMap.of();

    private final Object mutex = new Object();

    protected JsonObjectMapper(String name) {
        this(name, Defaults.ENABLED, Defaults.DYNAMIC, Defaults.PATH_TYPE);
    }

    protected JsonObjectMapper(String name, boolean enabled, boolean dynamic, JsonPath.Type pathType) {
        this(name, enabled, dynamic, pathType, Defaults.DATE_TIME_FORMATTERS);
    }

    protected JsonObjectMapper(String name, boolean enabled, boolean dynamic, JsonPath.Type pathType,
                               DateTimeFormatter[] dateTimeFormatters) {
        this(name, enabled, dynamic, pathType, dateTimeFormatters, null);
    }

    JsonObjectMapper(String name, boolean enabled, boolean dynamic, JsonPath.Type pathType,
                     DateTimeFormatter[] dateTimeFormatters, Map<String, JsonMapper> mappers) {
        this.name = name;
        this.enabled = enabled;
        this.dynamic = dynamic;
        this.pathType = pathType;
        this.dateTimeFormatters = dateTimeFormatters;
        if (mappers != null) {
            this.mappers = copyOf(mappers);
        }
    }

    @Override public String name() {
        return this.name;
    }

    public JsonObjectMapper putMapper(JsonMapper mapper) {
        synchronized (mutex) {
            mappers = newMapBuilder(mappers).put(mapper.name(), mapper).immutableMap();
        }
        return this;
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        for (JsonMapper mapper : mappers.values()) {
            mapper.traverse(fieldMapperListener);
        }
    }

    public void parse(JsonParseContext jsonContext) throws IOException {
        if (!enabled) {
            jsonContext.jp().skipChildren();
            return;
        }
        JsonParser jp = jsonContext.jp();

        JsonPath.Type origPathType = jsonContext.path().pathType();
        jsonContext.path().pathType(pathType);

        String currentFieldName = jp.getCurrentName();
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(jsonContext, currentFieldName);
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(jsonContext, currentFieldName);
            } else if (token == JsonToken.FIELD_NAME) {
                currentFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                serializeNullValue(jsonContext, currentFieldName);
            } else {
                serializeValue(jsonContext, currentFieldName, token);
            }
        }
        // restore the enable path flag
        jsonContext.path().pathType(origPathType);
    }

    private void serializeNullValue(JsonParseContext jsonContext, String lastFieldName) throws IOException {
        // we can only handle null values if we have mappings for them
        JsonMapper mapper = mappers.get(lastFieldName);
        if (mapper != null) {
            mapper.parse(jsonContext);
        }
    }

    private void serializeObject(JsonParseContext jsonContext, String currentFieldName) throws IOException {
        jsonContext.path().add(currentFieldName);

        JsonMapper objectMapper = mappers.get(currentFieldName);
        if (objectMapper != null) {
            objectMapper.parse(jsonContext);
        } else {
            if (dynamic) {
                // we sync here just so we won't add it twice. Its not the end of the world
                // to sync here since next operations will get it before
                synchronized (mutex) {
                    objectMapper = mappers.get(currentFieldName);
                    if (objectMapper != null) {
                        objectMapper.parse(jsonContext);
                    }

                    BuilderContext builderContext = new BuilderContext(jsonContext.path());
                    objectMapper = JsonMapperBuilders.object(currentFieldName).enabled(true)
                            .dynamic(dynamic).pathType(pathType).dateTimeFormatter(dateTimeFormatters).build(builderContext);
                    putMapper(objectMapper);
                    objectMapper.parse(jsonContext);
                }
            } else {
                // not dynamic, read everything up to end object
                jsonContext.jp().skipChildren();
            }
        }

        jsonContext.path().remove();
    }

    private void serializeArray(JsonParseContext jsonContext, String lastFieldName) throws IOException {
        JsonParser jp = jsonContext.jp();
        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == JsonToken.START_OBJECT) {
                serializeObject(jsonContext, lastFieldName);
            } else if (token == JsonToken.START_ARRAY) {
                serializeArray(jsonContext, lastFieldName);
            } else if (token == JsonToken.FIELD_NAME) {
                lastFieldName = jp.getCurrentName();
            } else if (token == JsonToken.VALUE_NULL) {
                serializeNullValue(jsonContext, lastFieldName);
            } else {
                serializeValue(jsonContext, lastFieldName, token);
            }
        }
    }

    private void serializeValue(JsonParseContext jsonContext, String currentFieldName, JsonToken token) throws IOException {
        JsonMapper mapper = mappers.get(currentFieldName);
        if (mapper != null) {
            mapper.parse(jsonContext);
            return;
        }
        if (!dynamic) {
            return;
        }
        // we sync here since we don't want to add this field twice to the document mapper
        // its not the end of the world, since we add it to the mappers once we create it
        // so next time we won't even get here for this field
        synchronized (mutex) {
            mapper = mappers.get(currentFieldName);
            if (mapper != null) {
                mapper.parse(jsonContext);
                return;
            }

            BuilderContext builderContext = new BuilderContext(jsonContext.path());
            if (token == JsonToken.VALUE_STRING) {
                // check if it fits one of the date formats
                boolean isDate = false;
                for (DateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
                    try {
                        dateTimeFormatter.parseMillis(jsonContext.jp().getText());
                        mapper = dateField(currentFieldName).dateTimeFormatter(dateTimeFormatter).build(builderContext);
                        isDate = true;
                        break;
                    } catch (Exception e) {
                        // failure to parse this, continue
                    }
                }
                if (!isDate) {
                    mapper = stringField(currentFieldName).build(builderContext);
                }
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                mapper = longField(currentFieldName).build(builderContext);
            } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
                mapper = doubleField(currentFieldName).build(builderContext);
            } else if (token == JsonToken.VALUE_TRUE) {
                mapper = booleanField(currentFieldName).build(builderContext);
            } else if (token == JsonToken.VALUE_FALSE) {
                mapper = booleanField(currentFieldName).build(builderContext);
            } else {
                // TODO how do we identify dynamically that its a binary value?
                throw new ElasticSearchIllegalStateException("Can't handle serializing a dynamic type with json token [" + token + "] and field name [" + currentFieldName + "]");
            }
            putMapper(mapper);
            jsonContext.docMapper().addFieldMapper((FieldMapper) mapper);

            mapper.parse(jsonContext);
        }
    }
}
