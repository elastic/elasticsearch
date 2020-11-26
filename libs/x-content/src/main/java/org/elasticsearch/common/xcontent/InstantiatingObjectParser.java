/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Like {@link ConstructingObjectParser} but works with objects which have a constructor that matches declared fields.
 * <p>
 * Declaring a {@linkplain InstantiatingObjectParser} is intentionally quite similar to declaring an {@linkplain ConstructingObjectParser}
 * with two important differences.
 * <p>
 * The main differences being that it is using Builder to construct the parser and takes a class of the target object instead of the object
 * builder. The target object must have exactly one constructor with the number and order of arguments matching the number of order of
 * declared fields. If there are more then 2 constructors with the same number of arguments, one of them needs to be marked with
 * {@linkplain ParserConstructor} annotation.
 * <pre>{@code
 *   public static class Thing{
 *       public Thing(String animal, String vegetable, int mineral) {
 *           ....
 *       }
 *
 *       public void setFruit(int fruit) { ... }
 *
 *       public void setBug(int bug) { ... }
 *
 *   }
 *
 *   private static final InstantiatingObjectParser<Thing, SomeContext> PARSER = new InstantiatingObjectParser<>("thing", Thing.class);
 *   static {
 *       PARSER.declareString(constructorArg(), new ParseField("animal"));
 *       PARSER.declareString(constructorArg(), new ParseField("vegetable"));
 *       PARSER.declareInt(optionalConstructorArg(), new ParseField("mineral"));
 *       PARSER.declareInt(Thing::setFruit, new ParseField("fruit"));
 *       PARSER.declareInt(Thing::setBug, new ParseField("bug"));
 *       PARSER.finalizeFields()
 *   }
 * }</pre>
 */
public class InstantiatingObjectParser<Value, Context>
    implements BiFunction<XContentParser, Context, Value>, ContextParser<Context, Value> {

    public static <Value, Context> Builder<Value, Context> builder(String name, boolean ignoreUnknownFields, Class<Value> valueClass) {
        return new Builder<>(name, ignoreUnknownFields, valueClass);
    }

    public static <Value, Context> Builder<Value, Context> builder(String name, Class<Value> valueClass) {
        return new Builder<>(name, valueClass);
    }

    public static class Builder<Value, Context> extends AbstractObjectParser<Value, Context> {

        private final ConstructingObjectParser<Value, Context> constructingObjectParser;

        private final Class<Value> valueClass;

        private Constructor<Value> constructor;

        public Builder(String name, Class<Value> valueClass) {
            this(name, false, valueClass);
        }

        public Builder(String name, boolean ignoreUnknownFields, Class<Value> valueClass) {
            this.constructingObjectParser = new ConstructingObjectParser<>(name, ignoreUnknownFields, this::build);
            this.valueClass = valueClass;
        }

        @SuppressWarnings("unchecked")
        public InstantiatingObjectParser<Value, Context> build() {
            Constructor<?> constructor = null;
            int neededArguments = constructingObjectParser.getNumberOfFields();
            // Try to find an annotated constructor
            for (Constructor<?> c : valueClass.getConstructors()) {
                if (c.getAnnotation(ParserConstructor.class) != null) {
                    if (constructor != null) {
                        throw new IllegalArgumentException("More then one public constructor with @ParserConstructor annotation exist in " +
                            "the class " + valueClass.getName());
                    }
                    if (c.getParameterCount() != neededArguments) {
                        throw new IllegalArgumentException("Annotated constructor doesn't have " + neededArguments +
                            " arguments in the class " + valueClass.getName());
                    }
                    constructor = c;
                }
            }
            if (constructor == null) {
                // fallback to a constructor with required number of arguments
                for (Constructor<?> c : valueClass.getConstructors()) {
                    if (c.getParameterCount() == neededArguments) {
                        if (constructor != null) {
                            throw new IllegalArgumentException("More then one public constructor with " + neededArguments +
                                " arguments found. The use of @ParserConstructor annotation is required for class " + valueClass.getName());
                        }
                        constructor = c;
                    }
                }
            }
            if (constructor == null) {
                throw new IllegalArgumentException("No public constructors with " + neededArguments + " parameters exist in the class " +
                    valueClass.getName());
            }
            this.constructor = (Constructor<Value>) constructor;
            return new InstantiatingObjectParser<>(constructingObjectParser);
        }

        @Override
        public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField,
                                     ObjectParser.ValueType type) {
            constructingObjectParser.declareField(consumer, parser, parseField, type);
        }

        @Override
        public <T> void declareNamedObject(BiConsumer<Value, T> consumer, ObjectParser.NamedObjectParser<T, Context> namedObjectParser,
                                           ParseField parseField) {
            constructingObjectParser.declareNamedObject(consumer, namedObjectParser, parseField);
        }

        @Override
        public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer,
                                            ObjectParser.NamedObjectParser<T, Context> namedObjectParser, ParseField parseField) {
            constructingObjectParser.declareNamedObjects(consumer, namedObjectParser, parseField);
        }

        @Override
        public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer,
                                            ObjectParser.NamedObjectParser<T, Context> namedObjectParser,
                                            Consumer<Value> orderedModeCallback, ParseField parseField) {
            constructingObjectParser.declareNamedObjects(consumer, namedObjectParser, orderedModeCallback, parseField);
        }

        @Override
        public String getName() {
            return constructingObjectParser.getName();
        }

        @Override
        public void declareRequiredFieldSet(String... requiredSet) {
            constructingObjectParser.declareRequiredFieldSet(requiredSet);
        }

        @Override
        public void declareExclusiveFieldSet(String... exclusiveSet) {
            constructingObjectParser.declareExclusiveFieldSet(exclusiveSet);
        }

        private Value build(Object[] args) {
            if (constructor == null) {
                throw new IllegalArgumentException("InstantiatingObjectParser for type " + valueClass.getName() + " has to be finalized " +
                    "before the first use");
            }
            try {
                return constructor.newInstance(args);
            } catch (Exception ex) {
                throw new IllegalArgumentException("Cannot instantiate an object of " + valueClass.getName(), ex);
            }
        }
    }


    private final ConstructingObjectParser<Value, Context> constructingObjectParser;

    private InstantiatingObjectParser(ConstructingObjectParser<Value, Context> constructingObjectParser) {
        this.constructingObjectParser = constructingObjectParser;
    }

    @Override
    public Value parse(XContentParser parser, Context context) throws IOException {
        return constructingObjectParser.parse(parser, context);
    }

    @Override
    public Value apply(XContentParser xContentParser, Context context) {
        return constructingObjectParser.apply(xContentParser, context);
    }
}
