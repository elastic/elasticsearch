/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

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
 * declared fields. If there are more than 2 constructors with the same number of arguments, one of them needs to be marked with
 * {@linkplain ParserConstructor} annotation.
 *
 * It is also possible for the constructor to accept Context as the first parameter, in this case as in the case with multiple constructors
 * it is required for the constructor to be marked with {@linkplain ParserConstructor} annotation.
 *
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
 *   private static final InstantiatingObjectParser<Thing, SomeContext> PARSER;
 *   static {
 *       InstantiatingObjectParser.Builder<Thing, SomeContext> parser =
 *           InstantiatingObjectParser,builder<>("thing", true, Thing.class);
 *       parser.declareString(constructorArg(), new ParseField("animal"));
 *       parser.declareString(constructorArg(), new ParseField("vegetable"));
 *       parser.declareInt(optionalConstructorArg(), new ParseField("mineral"));
 *       parser.declareInt(Thing::setFruit, new ParseField("fruit"));
 *       parser.declareInt(Thing::setBug, new ParseField("bug"));
 *       PARSER = parser.build()
 *   }
 * }</pre>
 * <pre>{@code
 *
 *   public static class AnotherThing {
 *       @ParserConstructor
 *       public AnotherThing(SomeContext continent, String animal, String vegetable, int mineral) {
 *           ....
 *       }
 *   }
 *
 *   private static final InstantiatingObjectParser<AnotherThing, SomeContext> PARSER;
 *   static {
 *       InstantiatingObjectParser.Builder<AnotherThing, SomeContext> parser =
 *           InstantiatingObjectParser,builder<>("thing", true, AnotherThing.class);
 *       parser.declareString(constructorArg(), new ParseField("animal"));
 *       parser.declareString(constructorArg(), new ParseField("vegetable"));
 *       parser.declareInt(optionalConstructorArg(), new ParseField("mineral"));
 *       PARSER = parser.build()
 *   }
 * }</pre>
 */
public class InstantiatingObjectParser<Value, Context>
    implements
        BiFunction<XContentParser, Context, Value>,
        ContextParser<Context, Value> {

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
            this.constructingObjectParser = new ConstructingObjectParser<>(name, ignoreUnknownFields, this::buildInstance);
            this.valueClass = valueClass;
        }

        @SuppressWarnings({ "unchecked", "checkstyle:HiddenField" })
        public InstantiatingObjectParser<Value, Context> build() {
            Constructor<?> constructor = null;
            int neededArguments = constructingObjectParser.getNumberOfFields();
            // Try to find an annotated constructor
            for (Constructor<?> c : valueClass.getConstructors()) {
                if (c.getAnnotation(ParserConstructor.class) != null) {
                    if (constructor != null) {
                        throw new IllegalArgumentException(
                            "More then one public constructor with @ParserConstructor annotation exist in "
                                + "the class "
                                + valueClass.getName()
                        );
                    }
                    if (c.getParameterCount() < neededArguments || c.getParameterCount() > neededArguments + 1) {
                        throw new IllegalArgumentException(
                            "Annotated constructor doesn't have "
                                + neededArguments
                                + " or "
                                + (neededArguments + 1)
                                + " arguments in the class "
                                + valueClass.getName()
                        );
                    }
                    constructor = c;
                }
            }
            if (constructor == null) {
                // fallback to a constructor with required number of arguments
                for (Constructor<?> c : valueClass.getConstructors()) {
                    if (c.getParameterCount() == neededArguments) {
                        if (constructor != null) {
                            throw new IllegalArgumentException(
                                "More then one public constructor with "
                                    + neededArguments
                                    + " arguments found. The use of @ParserConstructor annotation is required for class "
                                    + valueClass.getName()
                            );
                        }
                        constructor = c;
                    }
                }
            }
            if (constructor == null) {
                throw new IllegalArgumentException(
                    "No public constructors with " + neededArguments + " parameters exist in the class " + valueClass.getName()
                );
            }
            this.constructor = (Constructor<Value>) constructor;
            return new InstantiatingObjectParser<>(constructingObjectParser);
        }

        @Override
        public <T> void declareField(
            BiConsumer<Value, T> consumer,
            ContextParser<Context, T> parser,
            ParseField parseField,
            ObjectParser.ValueType type
        ) {
            constructingObjectParser.declareField(consumer, parser, parseField, type);
        }

        @Override
        public <T> void declareNamedObject(
            BiConsumer<Value, T> consumer,
            ObjectParser.NamedObjectParser<T, Context> namedObjectParser,
            ParseField parseField
        ) {
            constructingObjectParser.declareNamedObject(consumer, namedObjectParser, parseField);
        }

        @Override
        public <T> void declareNamedObjects(
            BiConsumer<Value, List<T>> consumer,
            ObjectParser.NamedObjectParser<T, Context> namedObjectParser,
            ParseField parseField
        ) {
            constructingObjectParser.declareNamedObjects(consumer, namedObjectParser, parseField);
        }

        @Override
        public <T> void declareNamedObjects(
            BiConsumer<Value, List<T>> consumer,
            ObjectParser.NamedObjectParser<T, Context> namedObjectParser,
            Consumer<Value> orderedModeCallback,
            ParseField parseField
        ) {
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

        private Value buildInstance(Object[] args, Context context) {
            if (constructor == null) {
                throw new IllegalArgumentException(
                    "InstantiatingObjectParser for type " + valueClass.getName() + " has to be finalized " + "before the first use"
                );
            }
            try {
                if (constructor.getParameterCount() != args.length) {
                    Object[] newArgs = new Object[args.length + 1];
                    System.arraycopy(args, 0, newArgs, 1, args.length);
                    newArgs[0] = context;
                    return constructor.newInstance(newArgs);
                } else {
                    return constructor.newInstance(args);
                }
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
