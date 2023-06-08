/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tree;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolution;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.NodeSubclassTests;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class EsqlNodeSubclassTests<T extends B, B extends Node<B>> extends NodeSubclassTests<T, B> {

    private static final List<Class<?>> CLASSES_WITH_MIN_TWO_CHILDREN = List.of(Concat.class, CIDRMatch.class);

    // List of classes that are "unresolved" NamedExpression subclasses, therefore not suitable for use with logical/physical plan nodes.
    private static final List<Class<?>> UNRESOLVED_CLASSES = List.of(
        UnresolvedAttribute.class,
        UnresolvedAlias.class,
        UnresolvedException.class,
        UnresolvedFunction.class,
        UnresolvedNamedExpression.class,
        UnresolvedStar.class
    );

    public EsqlNodeSubclassTests(Class<T> subclass) {
        super(subclass);
    }

    @Override
    protected Object pluggableMakeArg(Class<? extends Node<?>> toBuildClass, Class<?> argClass) throws Exception {
        if (argClass == Dissect.Parser.class) {
            // Dissect.Parser is a record / final, cannot be mocked
            String pattern = randomDissectPattern();
            String appendSeparator = randomAlphaOfLength(16);
            return new Dissect.Parser(pattern, appendSeparator, new DissectParser(pattern, appendSeparator));
        } else if (argClass == Grok.Parser.class) {
            // Grok.Parser is a record / final, cannot be mocked
            return Grok.pattern(Source.EMPTY, randomGrokPattern());
        } else if (argClass == EsQueryExec.FieldSort.class) {
            return randomFieldSort();
        } else if (toBuildClass == Pow.class && Expression.class.isAssignableFrom(argClass)) {
            return randomResolvedExpression(randomBoolean() ? FieldAttribute.class : Literal.class);
        } else if (isPlanNodeClass(toBuildClass) && Expression.class.isAssignableFrom(argClass)) {
            return randomResolvedExpression(argClass);
        } else if (argClass == EnrichPolicyResolution.class) {
            // EnrichPolicyResolution is a record
            return new EnrichPolicyResolution(
                randomAlphaOfLength(5),
                new EnrichPolicy(
                    randomAlphaOfLength(10),
                    null,
                    List.of(randomAlphaOfLength(5)),
                    randomAlphaOfLength(5),
                    List.of(randomAlphaOfLength(5), randomAlphaOfLength(5))
                ),
                IndexResolution.invalid(randomAlphaOfLength(5))
            );
        }

        return null;
    }

    @Override
    protected Object pluggableMakeParameterizedArg(Class<? extends Node<?>> toBuildClass, ParameterizedType pt) {
        if (toBuildClass == OutputExec.class && pt.getRawType() == Consumer.class) {
            // pageConsumer just needs a BiConsumer. But the consumer has to have reasonable
            // `equals` for randomValueOtherThan, so we just ensure that a new instance is
            // created each time which uses Object::equals identity.
            return new Consumer<Page>() {
                @Override
                public void accept(Page page) {
                    // do nothing
                }
            };
        }
        return null;
    }

    @Override
    protected boolean hasAtLeastTwoChildren(Class<? extends Node<?>> toBuildClass) {
        return CLASSES_WITH_MIN_TWO_CHILDREN.stream().anyMatch(toBuildClass::equals);
    }

    static final Predicate<String> CLASSNAME_FILTER = className -> (className.startsWith("org.elasticsearch.xpack.ql") != false
        || className.startsWith("org.elasticsearch.xpack.esql") != false);

    @Override
    protected Predicate<String> pluggableClassNameFilter() {
        return CLASSNAME_FILTER;
    }

    /** Scans the {@code .class} files to identify all classes and checks if they are subclasses of {@link Node}. */
    @ParametersFactory
    @SuppressWarnings("rawtypes")
    public static List<Object[]> nodeSubclasses() throws IOException {
        return subclassesOf(Node.class, CLASSNAME_FILTER).stream()
            .filter(c -> testClassFor(c) == null)
            .map(c -> new Object[] { c })
            .toList();
    }

    static boolean isPlanNodeClass(Class<? extends Node<?>> toBuildClass) {
        return PhysicalPlan.class.isAssignableFrom(toBuildClass) || LogicalPlan.class.isAssignableFrom(toBuildClass);
    }

    Expression randomResolvedExpression(Class<?> argClass) throws Exception {
        assert Expression.class.isAssignableFrom(argClass);
        @SuppressWarnings("unchecked")
        Class<? extends Expression> asNodeSubclass = (Class<? extends Expression>) argClass;
        if (Modifier.isAbstract(argClass.getModifiers())) {
            while (true) {
                var candidate = randomFrom(subclassesOf(asNodeSubclass));
                if (UNRESOLVED_CLASSES.contains(candidate) == false) {
                    asNodeSubclass = candidate;
                    break;
                }
            }
        }
        return makeNode(asNodeSubclass);
    }

    static String randomDissectPattern() {
        return randomFrom(Set.of("%{a} %{b}", "%{b} %{c}", "%{a} %{b} %{c}", "%{b} %{c} %{d}", "%{x}"));
    }

    static String randomGrokPattern() {
        return randomFrom(
            Set.of("%{NUMBER:b:int} %{NUMBER:c:float} %{NUMBER:d:double} %{WORD:e:boolean}", "[a-zA-Z0-9._-]+", "%{LOGLEVEL}")
        );
    }

    static List<DataType> DATA_TYPES = EsqlDataTypes.types().stream().toList();

    static EsQueryExec.FieldSort randomFieldSort() {
        return new EsQueryExec.FieldSort(
            field(randomAlphaOfLength(16), randomFrom(DATA_TYPES)),
            randomFrom(EnumSet.allOf(Order.OrderDirection.class)),
            randomFrom(EnumSet.allOf(Order.NullsPosition.class))
        );
    }

    static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Collections.emptyMap(), false));
    }
}
