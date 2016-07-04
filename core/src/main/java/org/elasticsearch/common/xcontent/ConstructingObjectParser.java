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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Like {@link ObjectParser} but works with objects that have constructors whose arguments are mixed in with its other settings. Queries are
 * like this, for example <code>ids</code> requires <code>types</code> but always parses the <code>values</code> field on the same level. If
 * this doesn't sounds like what you want to parse have a look at
 * {@link ObjectParser#declareNamedObjects(BiConsumer, ObjectParser.NamedObjectParser, Consumer, ParseField)} which solves a slightly
 * different but similar sounding problem.
 * <p>
 * Anyway, {@linkplain ConstructingObjectParser} parses the fields in the order that they are in the XContent, collecting constructor
 * arguments and parsing and queueing normal fields until all constructor arguments are parsed. Then it builds the target object and replays
 * the queued fields. Any fields that come in after the last constructor arguments are parsed and immediately applied to the target object
 * just like {@linkplain ObjectParser}.
 * </p>
 * <p>
 * Declaring a {@linkplain ConstructingObjectParser} is intentionally quite similar to declaring an {@linkplain ObjectParser}. The only
 * differences being that constructor arguments are declared with the consumer returned by the static {@link #constructorArg()} method and
 * that {@linkplain ConstructingObjectParser}'s constructor takes a lambda that must build the target object from a list of constructor
 * arguments:
 * </p>
 * <pre>{@code
 *   private static final ConstructingObjectParser<Thing, SomeContext> PARSER = new ConstructingObjectParser<>("thing",
 *           a -> new Thing((String) a[0], (String) a[1], (Integer) a[2]));
 *   static {
 *       PARSER.declareString(constructorArg(), new ParseField("animal"));
 *       PARSER.declareString(constructorArg(), new ParseField("vegetable"));
 *       PARSER.declareInt(optionalConstructorArg(), new ParseField("mineral"));
 *       PARSER.declareInt(Thing::setFruit, new ParseField("fruit"));
 *       PARSER.declareInt(Thing::setBug, new ParseField("bug"));
 *   }
 * }</pre>
 * <p>
 * This does add some overhead compared to just using {@linkplain ObjectParser} directly. On a 2.2 GHz Intel Core i7 MacBook Air running on
 * battery power in a reasonably unscientific microbenchmark it is about 100 microseconds for a reasonably large object, less if the
 * constructor arguments are first. On this platform with the same microbenchmarks just creating the XContentParser is around 900
 * microseconds and using {#linkplain ObjectParser} directly adds another 300 or so microseconds. In the best case
 * {@linkplain ConstructingObjectParser} allocates two additional objects per parse compared to {#linkplain ObjectParser}. In the worst case
 * it allocates <code>3 + 2 * param_count</code> objects per parse. If this overhead is too much for you then feel free to have ObjectParser
 * parse a secondary object and have that one call the target object's constructor. That ought to be rare though.
 * </p>
 * <p>
 * Note: if optional constructor arguments aren't specified then the number of allocations is always the worst case.
 * </p>
 */
public final class ConstructingObjectParser<Value, Context extends ParseFieldMatcherSupplier> extends AbstractObjectParser<Value, Context> {
    /**
     * Consumer that marks a field as a required constructor argument instead of a real object field.
     */
    private static final BiConsumer<Object, Object> REQUIRED_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    /**
     * Consumer that marks a field as an optional constructor argument instead of a real object field.
     */
    private static final BiConsumer<Object, Object> OPTIONAL_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    /**
     * List of constructor names used for generating the error message if not all arrive.
     */
    private final List<ConstructorArgInfo> constructorArgInfos = new ArrayList<>();
    private final ObjectParser<Target, Context> objectParser;
    private final Function<Object[], Value> builder;
    /**
     * The number of fields on the targetObject. This doesn't include any constructor arguments and is the size used for the array backing
     * the field queue.
     */
    private int numberOfFields = 0;

    /**
     * Build the parser.
     * 
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param builder A function that builds the object from an array of Objects. Declare this inline with the parser, casting the elements
     *        of the array to the arguments so they work with your favorite constructor. The objects in the array will be in the same order
     *        that you declared the {{@link #constructorArg()}s and none will be null. If any of the constructor arguments aren't defined in
     *        the XContent then parsing will throw an error. We use an array here rather than a {@code Map<String, Object>} to save on
     *        allocations.
     */
    public ConstructingObjectParser(String name, Function<Object[], Value> builder) {
        objectParser = new ObjectParser<>(name);
        this.builder = builder;
    }

    /**
     * Call this to do the actual parsing. This implements {@link BiFunction} for conveniently integrating with ObjectParser.
     */
    @Override
    public Value apply(XContentParser parser, Context context) {
        try {
            return objectParser.parse(parser, new Target(parser), context).finish();
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "[" + objectParser.getName()  + "] failed to parse object", e);
        }
    }

    /**
     * Pass the {@linkplain BiConsumer} this returns the declare methods to declare a required constructor argument. See this class's
     * javadoc for an example. The order in which these are declared matters: it is the order that they come in the array passed to
     * {@link #builder} and the order that missing arguments are reported to the user if any are missing. When all of these parameters are
     * parsed from the {@linkplain XContentParser} the target object is immediately built.
     */
    @SuppressWarnings("unchecked") // Safe because we never call the method. This is just trickery to make the interface pretty.
    public static <Value, FieldT> BiConsumer<Value, FieldT> constructorArg() {
        return (BiConsumer<Value, FieldT>) REQUIRED_CONSTRUCTOR_ARG_MARKER;
    }

    /**
     * Pass the {@linkplain BiConsumer} this returns the declare methods to declare an optional constructor argument. See this class's
     * javadoc for an example. The order in which these are declared matters: it is the order that they come in the array passed to
     * {@link #builder} and the order that missing arguments are reported to the user if any are missing. When all of these parameters are
     * parsed from the {@linkplain XContentParser} the target object is immediately built.
     */
    @SuppressWarnings("unchecked") // Safe because we never call the method. This is just trickery to make the interface pretty.
    public static <Value, FieldT> BiConsumer<Value, FieldT> optionalConstructorArg() {
        return (BiConsumer<Value, FieldT>) OPTIONAL_CONSTRUCTOR_ARG_MARKER;
    }

    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField, ValueType type) {
        if (consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER || consumer == OPTIONAL_CONSTRUCTOR_ARG_MARKER) {
            /*
             * Constructor arguments are detected by this "marker" consumer. It keeps the API looking clean even if it is a bit sleezy. We
             * then build a new consumer directly against the object parser that triggers the "constructor arg just arrived behavior" of the
             * parser. Conveniently, we can close over the position of the constructor in the argument list so we don't need to do any fancy
             * or expensive lookups whenever the constructor args come in.
             */
            int position = constructorArgInfos.size();
            boolean required = consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER;
            constructorArgInfos.add(new ConstructorArgInfo(parseField, required));
            objectParser.declareField((target, v) -> target.constructorArg(position, parseField, v), parser, parseField, type);
        } else {
            numberOfFields += 1;
            objectParser.declareField(queueingConsumer(consumer, parseField), parser, parseField, type);
        }
    }

    /**
     * Creates the consumer that does the "field just arrived" behavior. If the targetObject hasn't been built then it queues the value.
     * Otherwise it just applies the value just like {@linkplain ObjectParser} does.
     */
    private <T> BiConsumer<Target, T> queueingConsumer(BiConsumer<Value, T> consumer, ParseField parseField) {
        return (target, v) -> {
            if (target.targetObject != null) {
                // The target has already been built. Just apply the consumer now.
                consumer.accept(target.targetObject, v);
                return;
            }
            /*
             * The target hasn't been built. Queue the consumer. The next two lines are the only allocations that ConstructingObjectParser
             * does during parsing other than the boxing the ObjectParser might do. The first one is to preserve a snapshot of the current
             * location so we can add it to the error message if parsing fails. The second one (the lambda) is the actual operation being
             * queued. Note that we don't do any of this if the target object has already been built.
             */
            XContentLocation location = target.parser.getTokenLocation();
            target.queue(targetObject -> {
                try {
                    consumer.accept(targetObject, v);
                } catch (Exception e) {
                    throw new ParsingException(location,
                            "[" + objectParser.getName() + "] failed to parse field [" + parseField.getPreferredName() + "]", e);
                }
            });
        };
    }

    /**
     * The target of the {@linkplain ConstructingObjectParser}. One of these is built every time you call
     * {@linkplain ConstructingObjectParser#apply(XContentParser, ParseFieldMatcherSupplier)} Note that it is not static so it inherits
     * {@linkplain ConstructingObjectParser}'s type parameters.
     */
    private class Target {
        /**
         * Array of constructor args to be passed to the {@link ConstructingObjectParser#builder}.
         */
        private final Object[] constructorArgs = new Object[constructorArgInfos.size()];
        /**
         * The parser this class is working against. We store it here so we can fetch it conveniently when queueing fields to lookup the
         * location of each field so that we can give a useful error message when replaying the queue.
         */
        private final XContentParser parser;
        /**
         * How many of the constructor parameters have we collected? We keep track of this so we don't have to count the
         * {@link #constructorArgs} array looking for nulls when we receive another constructor parameter. When this is equal to the size of
         * {@link #constructorArgs} we build the target object.
         */
        private int constructorArgsCollected = 0;
        /**
         * Fields to be saved to the target object when we can build it. This is only allocated if a field has to be queued.
         */
        private Consumer<Value>[] queuedFields;
        /**
         * The count of fields already queued.
         */
        private int queuedFieldsCount = 0;
        /**
         * The target object. This will be instantiated with the constructor arguments are all parsed.
         */
        private Value targetObject;

        public Target(XContentParser parser) {
            this.parser = parser;
        }

        /**
         * Set a constructor argument and build the target object if all constructor arguments have arrived.
         */
        private void constructorArg(int position, ParseField parseField, Object value) {
            if (constructorArgs[position] != null) {
                throw new IllegalArgumentException("Can't repeat param [" + parseField + "]");
            }
            constructorArgs[position] = value;
            constructorArgsCollected++;
            if (constructorArgsCollected == constructorArgInfos.size()) {
                buildTarget();
            }
        }

        /**
         * Queue a consumer that we'll call once the targetObject is built. If targetObject has been built this will fail because the caller
         * should have just applied the consumer immediately.
         */
        private void queue(Consumer<Value> queueMe) {
            assert targetObject == null: "Don't queue after the targetObject has been built! Just apply the consumer directly.";
            if (queuedFields == null) {
                @SuppressWarnings("unchecked")
                Consumer<Value>[] queuedFields = new Consumer[numberOfFields];
                this.queuedFields = queuedFields;
            }
            queuedFields[queuedFieldsCount] = queueMe;
            queuedFieldsCount++;
        }

        /**
         * Finish parsing the object. 
         */
        private Value finish() {
            if (targetObject != null) {
                return targetObject;
            }
            /*
             * The object hasn't been built which ought to mean we're missing some constructor arguments. But they could be optional! We'll
             * check if they are all optional and build the error message at the same time - if we don't start the error message then they
             * were all optional!
             */
            StringBuilder message = null;
            for (int i = 0; i < constructorArgs.length; i++) {
                if (constructorArgs[i] != null) continue;
                ConstructorArgInfo arg = constructorArgInfos.get(i);
                if (false == arg.required) continue;
                if (message == null) {
                    message = new StringBuilder("Required [").append(arg.field);
                } else {
                    message.append(", ").append(arg.field);
                }
            }
            if (message != null) {
                // There were non-optional constructor arguments missing.
                throw new IllegalArgumentException(message.append(']').toString());
            }
            /*
             * If there weren't any constructor arguments declared at all then we won't get an error message but this isn't really a valid
             * use of ConstructingObjectParser. You should be using ObjectParser instead. Since this is more of a programmer error and the
             * parser ought to still work we just assert this.
             */
            assert false == constructorArgInfos.isEmpty() : "[" + objectParser.getName() + "] must configure at least on constructor "
                        + "argument. If it doesn't have any it should use ObjectParser instead of ConstructingObjectParser. This is a bug "
                        + "in the parser declaration.";
            // All missing constructor arguments were optional. Just build the target and return it.
            buildTarget();
            return targetObject;
        }

        private void buildTarget() {
            try {
                targetObject = builder.apply(constructorArgs);
                while (queuedFieldsCount > 0) {
                    queuedFieldsCount -= 1;
                    queuedFields[queuedFieldsCount].accept(targetObject);
                }
            } catch (ParsingException e) {
                throw new ParsingException(e.getLineNumber(), e.getColumnNumber(),
                        "failed to build [" + objectParser.getName() + "] after last required field arrived", e);
            } catch (Exception e) {
                throw new ParsingException(null, "Failed to build [" + objectParser.getName() + "] after last required field arrived", e);
            }
        }
    }

    private static class ConstructorArgInfo {
        final ParseField field;
        final boolean required;

        public ConstructorArgInfo(ParseField field, boolean required) {
            this.field = field;
            this.required = required;
        }
    }
}
