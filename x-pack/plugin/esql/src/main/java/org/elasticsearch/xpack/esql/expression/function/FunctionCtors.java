/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.BinaryBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.BinaryConfigurationAwareBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.ConfigurationAwareBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.NaryBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.QuaternaryBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.QuaternaryConfigurationAwareBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.QuinaryBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.TernaryBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.TernaryConfigurationAwareBuilder;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition.UnaryConfigurationAwareBuilder;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Implementations of {@link FunctionDefinition.FunctionBuilder} for use with
 * {@link FunctionDefinition.Builder}. Methods named {@code *Ts} inject the
 * timestamp into the args for use with {@link TimestampAware} functions.
 */
class FunctionCtors {
    static FunctionDefinition.FunctionBuilder noArgs(java.util.function.Function<Source, ? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.apply(source);
    }

    static FunctionDefinition.FunctionBuilder noArgsConfig(ConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, cfg);
    }

    static FunctionDefinition.FunctionBuilder unaryConfig(UnaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), cfg);
    }

    static FunctionDefinition.FunctionBuilder unaryConfigTs(UnaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, UnresolvedTimestamp.withSource(source), cfg);
    }

    static FunctionDefinition.FunctionBuilder binaryConfig(BinaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), getOrNull(children, 1), cfg);
    }

    static FunctionDefinition.FunctionBuilder binaryConfigTs(BinaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), UnresolvedTimestamp.withSource(source), cfg);
    }

    static FunctionDefinition.FunctionBuilder ternaryConfig(TernaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), getOrNull(children, 1), getOrNull(children, 2), cfg);
    }

    static FunctionDefinition.FunctionBuilder quaternary(QuaternaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            children.get(1),
            getOrNull(children, 2),
            getOrNull(children, 3)
        );
    }

    static FunctionDefinition.FunctionBuilder quaternaryTs(QuaternaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            getOrNull(children, 1),
            getOrNull(children, 2),
            UnresolvedTimestamp.withSource(source)
        );
    }

    static FunctionDefinition.FunctionBuilder quaternaryConfig(QuaternaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            children.get(1),
            getOrNull(children, 2),
            getOrNull(children, 3),
            cfg
        );
    }

    static FunctionDefinition.FunctionBuilder quinary(QuinaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            getOrNull(children, 0),
            getOrNull(children, 1),
            getOrNull(children, 2),
            getOrNull(children, 3),
            getOrNull(children, 4)
        );
    }

    static FunctionDefinition.FunctionBuilder quinaryTs(QuinaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            getOrNull(children, 0),
            getOrNull(children, 1),
            getOrNull(children, 2),
            getOrNull(children, 3),
            UnresolvedTimestamp.withSource(source)
        );
    }

    static FunctionDefinition.FunctionBuilder nAry(NaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children);
    }

    static FunctionDefinition.FunctionBuilder unaryVariadic(FunctionDefinition.UnaryVariadicBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), children.subList(1, children.size()));
    }

    static FunctionDefinition.FunctionBuilder ternaryTsConfig(TernaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            getOrNull(children, 1),
            UnresolvedTimestamp.withSource(source),
            cfg
        );
    }

    static FunctionDefinition.FunctionBuilder quaternaryTsConfig(QuaternaryConfigurationAwareBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            getOrNull(children, 1),
            getOrNull(children, 2),
            UnresolvedTimestamp.withSource(source),
            cfg
        );
    }

    static FunctionDefinition.FunctionBuilder unary(BiFunction<Source, Expression, ? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.apply(source, children.getFirst());
    }

    static FunctionDefinition.FunctionBuilder unaryTs(BiFunction<Source, Expression, ? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.apply(source, UnresolvedTimestamp.withSource(source));
    }

    static FunctionDefinition.FunctionBuilder binary(BinaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), getOrNull(children, 1));
    }

    static FunctionDefinition.FunctionBuilder binaryTs(BinaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), UnresolvedTimestamp.withSource(source));
    }

    static FunctionDefinition.FunctionBuilder ternary(TernaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(source, children.getFirst(), getOrNull(children, 1), getOrNull(children, 2));
    }

    static FunctionDefinition.FunctionBuilder ternaryTs(TernaryBuilder<? extends Function> ctorRef) {
        return (source, children, cfg) -> ctorRef.build(
            source,
            children.getFirst(),
            getOrNull(children, 1),
            UnresolvedTimestamp.withSource(source)
        );
    }

    private static Expression getOrNull(List<Expression> children, int index) {
        return children.size() > index ? children.get(index) : null;
    }
}
