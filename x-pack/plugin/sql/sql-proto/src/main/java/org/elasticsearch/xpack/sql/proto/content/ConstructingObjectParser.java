/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.proto.content.ParserUtils.location;

/**
 * NB: cloned from the class with the same name in ES XContent.
 */
public class ConstructingObjectParser<Value, Context> extends AbstractObjectParser<Value, Context> {

    @SuppressWarnings("unchecked")
    public static <Value, FieldT> BiConsumer<Value, FieldT> constructorArg() {
        return (BiConsumer<Value, FieldT>) REQUIRED_CONSTRUCTOR_ARG_MARKER;
    }

    @SuppressWarnings("unchecked")
    public static <Value, FieldT> BiConsumer<Value, FieldT> optionalConstructorArg() {
        return (BiConsumer<Value, FieldT>) OPTIONAL_CONSTRUCTOR_ARG_MARKER;
    }

    private static final BiConsumer<?, ?> REQUIRED_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    private static final BiConsumer<?, ?> OPTIONAL_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    private final List<ConstructorArgInfo> constructorArgInfos = new ArrayList<>();
    private final ObjectParser<Target, Context> objectParser;
    private final BiFunction<Object[], Context, Value> builder;

    private int numberOfFields = 0;

    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, Function<Object[], Value> builder) {
        this(name, ignoreUnknownFields, (args, context) -> builder.apply(args));
    }

    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, BiFunction<Object[], Context, Value> builder) {
        objectParser = new ObjectParser<>(name, ignoreUnknownFields, null);
        this.builder = builder;
    }

    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, String field, ValueType type) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        if (field == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] is required");
        }

        if (isConstructorArg(consumer)) {
            int position = addConstructorArg(consumer, field);
            objectParser.declareField((target, v) -> target.constructorArg(position, v), parser, field, type);
        } else {
            numberOfFields += 1;
            objectParser.declareField(queueingConsumer(consumer, field), parser, field, type);
        }
    }

    private <T> BiConsumer<Target, T> queueingConsumer(BiConsumer<Value, T> consumer, String parseField) {
        return (target, v) -> {
            if (target.targetObject != null) {
                // The target has already been built. Just apply the consumer now.
                consumer.accept(target.targetObject, v);
                return;
            }
            JsonLocation location = target.parser.getTokenLocation();
            target.queue(targetObject -> {
                try {
                    consumer.accept(targetObject, v);
                } catch (Exception e) {
                    throw new ParseException(
                        location(location),
                        "[" + objectParser.getName() + "] failed to parse field [" + parseField + "]",
                        e
                    );
                }
            });
        };
    }

    private static boolean isConstructorArg(BiConsumer<?, ?> consumer) {
        return consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER || consumer == OPTIONAL_CONSTRUCTOR_ARG_MARKER;
    }

    private int addConstructorArg(BiConsumer<?, ?> consumer, String field) {
        constructorArgInfos.add(new ConstructorArgInfo(field, consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER));
        return constructorArgInfos.size() - 1;
    }

    @Override
    Value parse(JsonParser parser, Context context) throws IOException {
        return objectParser.parse(parser, new Target(parser, context), context).finish();
    }

    private class Target {
        private final Object[] constructorArgs;
        private final JsonParser parser;
        private final Context context;
        private int constructorArgsCollected = 0;
        private Consumer<Value>[] queuedFields;
        private Consumer<Value> queuedOrderedModeCallback;
        private int queuedFieldsCount = 0;
        private Value targetObject;

        Target(JsonParser parser, Context context) {
            this.parser = parser;
            this.context = context;
            this.constructorArgs = new Object[constructorArgInfos.size()];
        }

        private void constructorArg(int position, Object value) {
            constructorArgs[position] = value;
            constructorArgsCollected++;
            if (constructorArgsCollected == constructorArgInfos.size()) {
                buildTarget();
            }
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private void queue(Consumer<Value> queueMe) {
            assert targetObject == null : "Don't queue after the targetObject has been built! Just apply the consumer directly.";
            if (queuedFields == null) {
                this.queuedFields = (Consumer<Value>[]) new Consumer[numberOfFields];
            }
            queuedFields[queuedFieldsCount] = queueMe;
            queuedFieldsCount++;
        }

        private Value finish() {
            if (targetObject != null) {
                return targetObject;
            }

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
            if (constructorArgInfos.isEmpty()) {
                throw new ParseException(
                    "["
                        + objectParser.getName()
                        + "] must configure at least one constructor "
                        + "argument. If it doesn't have any it should use ObjectParser instead of ConstructingObjectParser. This is a bug "
                        + "in the parser declaration."
                );
            }
            // All missing constructor arguments were optional. Just build the target and return it.
            buildTarget();
            return targetObject;
        }

        private void buildTarget() {
            try {
                targetObject = builder.apply(constructorArgs, context);
                if (queuedOrderedModeCallback != null) {
                    queuedOrderedModeCallback.accept(targetObject);
                }
                while (queuedFieldsCount > 0) {
                    queuedFieldsCount -= 1;
                    queuedFields[queuedFieldsCount].accept(targetObject);
                }
            } catch (ParseException e) {
                throw new ParseException(
                    e.location(),
                    "failed to build [" + objectParser.getName() + "] after last required field arrived",
                    e
                );
            } catch (Exception e) {
                throw new ParseException(null, "Failed to build [" + objectParser.getName() + "] after last required field arrived", e);
            }
        }
    }

    private static class ConstructorArgInfo {
        final String field;
        final boolean required;

        ConstructorArgInfo(String field, boolean required) {
            this.field = field;
            this.required = required;
        }
    }
}
