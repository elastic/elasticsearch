/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public class FunctionDefinition {
    /**
     * Builds the function out of parameters.
     */
    @FunctionalInterface
    public interface FunctionBuilder {
        Function build(Source source, List<Expression> children, Configuration config);
    }

    /**
     * Create a builder for a {@link FunctionDefinition}.
     */
    public static <T extends Function> FunctionDefinition.Builder<T> def(Class<T> function) {
        return new Builder<>(function);
    }

    private final String name;
    private final List<String> aliases;
    private final Class<? extends Function> clazz;
    private final BiConsumer<Source, List<Expression>> validate;
    private final FunctionBuilder builder;
    private final List<String> subCapabilities;

    private FunctionDefinition(
        String name,
        List<String> aliases,
        Class<? extends Function> clazz,
        BiConsumer<Source, List<Expression>> validate,
        FunctionBuilder builder,
        List<String> subCapabilities
    ) {
        this.name = name;
        this.aliases = aliases;
        this.clazz = clazz;
        this.validate = validate;
        this.builder = builder;
        this.subCapabilities = subCapabilities;
    }

    public String name() {
        return name;
    }

    public List<String> aliases() {
        return aliases;
    }

    public Class<? extends Function> clazz() {
        return clazz;
    }

    /**
     * Build the {@link Function} from an {@link UnresolvedFunction}.
     */
    public Function build(UnresolvedFunction uf, Configuration configuration) {
        try {
            validate.accept(uf.source(), uf.children());
            return builder.build(uf.source(), uf.children(), configuration);
        } catch (QlIllegalArgumentException e) {
            throw new ParsingException(e, uf.source(), "error building [{}]: {}", name, e.getMessage());
        }
    }

    public List<String> capabilities() {
        return subCapabilities;
    }

    @Override
    public String toString() {
        return format(null, "{}({})", name, aliases.isEmpty() ? "" : aliases.size() == 1 ? aliases.get(0) : aliases);
    }

    /**
     * A builder for {@link FunctionDefinition}s. Get one from {@link #def}.
     */
    public static class Builder<T extends Function> {
        private final Class<T> function;
        private BiConsumer<Source, List<Expression>> validate;
        private FunctionDefinition.FunctionBuilder builder;
        private List<String> capabilities = List.of();

        Builder(Class<T> function) {
            this.function = function;
        }

        /**
         * Adds capabilities to mark changes or fixes to the function. Use it like:
         * {@snippet :
         * public static final FunctionDefinition DEFINITION = FunctionDefinition.def(IpPrefix.class)
         *     .ternary(IpPrefix::new)
         *     // Fix a bug leading to the scratch leaking data to other rows.
         *     .capabilities("fix_dirty_scratch_leak")
         *     .name("ip_prefix");
         * }
         */
        public Builder<T> capabilities(String... capabilities) {
            this.capabilities = List.of(capabilities);
            return this;
        }

        /**
         * Build the {@link FunctionDefinition} with the given primary name and optional aliases.
         */
        public FunctionDefinition name(String name, String... aliases) {
            return new FunctionDefinition(name, List.of(aliases), function, validate, builder, capabilities);
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a no-argument function.
         */
        public Builder<T> noArgs(java.util.function.Function<Source, T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                throw new IllegalArgumentException("timestamp aware functions not supported here");
            }
            validate = FunctionArgumentValidation.NO_ARGS;
            builder = FunctionCtors.noArgs(ctorRef);
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a no-argument function
         * that needs {@link Configuration}.
         */
        public Builder<T> noArgs(FunctionDefinition.ConfigurationAwareBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                throw new IllegalArgumentException("timestamp aware functions not supported here");
            }
            validate = FunctionArgumentValidation.NO_ARGS;
            builder = FunctionCtors.noArgsConfig(ctorRef);
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a unary function.
         */
        public Builder<T> unary(BiFunction<Source, Expression, T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.NO_ARGS;
                builder = FunctionCtors.unaryTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.UNARY;
                builder = FunctionCtors.unary(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a unary function that needs {@link Configuration}.
         */
        public Builder<T> unaryConfig(FunctionDefinition.UnaryConfigurationAwareBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.NO_ARGS;
                builder = FunctionCtors.unaryConfigTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.UNARY;
                builder = FunctionCtors.unaryConfig(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a function with one required argument followed by
         * zero or more variadic arguments.
         */
        public Builder<T> unaryVariadic(FunctionDefinition.UnaryVariadicBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                throw new IllegalArgumentException("timestamp aware functions not supported here");
            }
            validate = FunctionArgumentValidation.unaryVariadic(function);
            builder = FunctionCtors.unaryVariadic(ctorRef);
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a binary function.
         */
        public Builder<T> binary(FunctionDefinition.BinaryBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.UNARY;
                builder = FunctionCtors.binaryTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.binary(function);
                builder = FunctionCtors.binary(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a binary function that needs {@link Configuration}.
         */
        public Builder<T> binaryConfig(FunctionDefinition.BinaryConfigurationAwareBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.UNARY;
                builder = FunctionCtors.binaryConfigTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.binary(function);
                builder = FunctionCtors.binaryConfig(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a ternary function.
         */
        public Builder<T> ternary(FunctionDefinition.TernaryBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.binary(function);
                builder = FunctionCtors.ternaryTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.ternary(function);
                builder = FunctionCtors.ternary(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a ternary function that needs {@link Configuration}.
         */
        public Builder<T> ternaryConfig(FunctionDefinition.TernaryConfigurationAwareBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.binary(function);
                builder = FunctionCtors.ternaryTsConfig(ctorRef);
            } else {
                validate = FunctionArgumentValidation.ternary(function);
                builder = FunctionCtors.ternaryConfig(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a quaternary function.
         */
        public Builder<T> quaternary(FunctionDefinition.QuaternaryBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.ternary(function);
                builder = FunctionCtors.quaternaryTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.quaternary(function);
                builder = FunctionCtors.quaternary(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a quaternary function that needs {@link Configuration}.
         */
        public Builder<T> quaternaryConfig(FunctionDefinition.QuaternaryConfigurationAwareBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.ternary(function);
                builder = FunctionCtors.quaternaryTsConfig(ctorRef);
            } else {
                validate = FunctionArgumentValidation.quaternary(function);
                builder = FunctionCtors.quaternaryConfig(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a quinary function.
         */
        public Builder<T> quinary(FunctionDefinition.QuinaryBuilder<T> ctorRef, int numOptionalParams) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                validate = FunctionArgumentValidation.quinaryTs(function, numOptionalParams);
                builder = FunctionCtors.quinaryTs(ctorRef);
            } else {
                validate = FunctionArgumentValidation.quinary(function, numOptionalParams);
                builder = FunctionCtors.quinary(ctorRef);
            }
            return this;
        }

        /**
         * Build a {@linkplain FunctionDefinition} for a function with any number of arguments.
         */
        public Builder<T> nAry(FunctionDefinition.NaryBuilder<T> ctorRef) {
            if (TimestampAware.class.isAssignableFrom(function)) {
                throw new IllegalArgumentException("timestamp aware functions not supported here");
            }
            validate = (source, children) -> {};
            builder = FunctionCtors.nAry(ctorRef);
            return this;
        }
    }

    // Builder functional interfaces
    public interface ConfigurationAwareBuilder<T> {
        T build(Source source, Configuration configuration);
    }

    public interface UnaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression exp, Configuration configuration);
    }

    public interface UnaryVariadicBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic);
    }

    public interface BinaryBuilder<T> {
        T build(Source source, Expression left, Expression right);
    }

    public interface BinaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression left, Expression right, Configuration configuration);
    }

    public interface BinaryVariadicWithOptionsBuilder<T> {
        T build(Source source, Expression exp, List<Expression> variadic, Expression options);
    }

    public interface TernaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three);
    }

    public interface TernaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Configuration configuration);
    }

    public interface QuaternaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four);
    }

    public interface QuaternaryConfigurationAwareBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four, Configuration configuration);
    }

    public interface QuinaryBuilder<T> {
        T build(Source source, Expression one, Expression two, Expression three, Expression four, Expression five);
    }

    public interface NaryBuilder<T> {
        T build(Source source, List<Expression> children);
    }

}
