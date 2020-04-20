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
public final class ConstructingObjectParser<Value, Context> extends AbstractConstructingObjectParser<Value, Context> {
    private final BiFunction<Object[], Context, Value> builder;

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param builder A function that builds the object from an array of Objects. Declare this inline with the parser, casting the elements
     *        of the array to the arguments so they work with your favorite constructor. The objects in the array will be in the same order
     *        that you declared the {@link #constructorArg()}s and none will be null. If any of the constructor arguments aren't defined in
     *        the XContent then parsing will throw an error. We use an array here rather than a {@code Map<String, Object>} to save on
     *        allocations.
     */
    public ConstructingObjectParser(String name, Function<Object[], Value> builder) {
        this(name, false, builder);
    }

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param ignoreUnknownFields Should this parser ignore unknown fields? This should generally be set to true only when parsing responses
     *        from external systems, never when parsing requests from users.
     * @param builder A function that builds the object from an array of Objects. Declare this inline with the parser, casting the elements
     *        of the array to the arguments so they work with your favorite constructor. The objects in the array will be in the same order
     *        that you declared the {@link #constructorArg()}s and none will be null. If any of the constructor arguments aren't defined in
     *        the XContent then parsing will throw an error. We use an array here rather than a {@code Map<String, Object>} to save on
     *        allocations.
     */
    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, Function<Object[], Value> builder) {
        this(name, ignoreUnknownFields, (args, context) -> builder.apply(args));
    }

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param ignoreUnknownFields Should this parser ignore unknown fields? This should generally be set to true only when parsing responses
     *        from external systems, never when parsing requests from users.
     * @param builder A binary function that builds the object from an array of Objects and the parser context.  Declare this inline with
     *        the parser, casting the elements of the array to the arguments so they work with your favorite constructor. The objects in
     *        the array will be in the same order that you declared the {@link #constructorArg()}s and none will be null. The second
     *        argument is the value of the context provided to the {@link #parse(XContentParser, Object) parse function}. If any of the
     *        constructor arguments aren't defined in the XContent then parsing will throw an error. We use an array here rather than a
     *        {@code Map<String, Object>} to save on allocations.
     */
    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, BiFunction<Object[], Context, Value> builder) {
        super(name, ignoreUnknownFields);
        this.builder = builder;

    }


    @Override
    protected Value build(Object[] args, Context context) {
        return builder.apply(args, context);
    }
}
