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
import org.elasticsearch.xpack.esql.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttributeTests;
import org.elasticsearch.xpack.ql.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.CompoundAggregate;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AggExtractorInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.BinaryPipesTests;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.fulltext.FullTextPredicate;
import org.elasticsearch.xpack.ql.expression.predicate.regex.Like;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.NodeSubclassTests;
import org.elasticsearch.xpack.ql.tree.NodeTests;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.tree.SourceTests;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;
import org.mockito.exceptions.base.MockitoException;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.time.ZoneId;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class EsqlNodeSubclassTests<T extends B, B extends Node<B>> extends NodeSubclassTests<T, B> {

    private static final List<Class<?>> CLASSES_WITH_MIN_TWO_CHILDREN = List.of(Concat.class, CIDRMatch.class);

    // List of classes that are "unresolved" NamedExpression subclasses, therefore not suitable for use with logical/physical plan nodes.
    private static final List<Class<?>> UNRESOLVED_CLASSES = List.of(
        UnresolvedAttribute.class,
        UnresolvedAlias.class,
        UnresolvedException.class,
        UnresolvedFunction.class,
        UnresolvedNamedExpression.class,
        UnresolvedNamePattern.class,
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
        } else if (argClass == Stat.class) {
            // record field
            return new Stat(randomRealisticUnicodeOfLength(10), randomFrom(StatsType.values()), null);
        } else if (argClass == Integer.class) {
            return randomInt();
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

    private static final List<String> EXCLUDED_QL_PREFIXES = List.of(
        "org.elasticsearch.xpack.ql.expression.FieldAttribute",
        "org.elasticsearch.xpack.ql.expression.function.aggregate."
    );

    static final Predicate<String> CLASSNAME_FILTER = className -> ((className.startsWith("org.elasticsearch.xpack.ql") != false
        || className.startsWith("org.elasticsearch.xpack.esql") != false)
        && EXCLUDED_QL_PREFIXES.stream().anyMatch(prefix -> className.startsWith(prefix)) == false);

    @Override
    protected Predicate<String> pluggableClassNameFilter() {
        return CLASSNAME_FILTER;
    }

    /**
     * Make an argument to feed to the constructor for {@code toBuildClass}.
     */
    @SuppressWarnings("unchecked")
    protected Object makeArg(Class<? extends Node<?>> toBuildClass, Type argType) throws Exception {

        if (argType instanceof ParameterizedType pt) {
            if (pt.getRawType() == Map.class) {
                return makeMap(toBuildClass, pt);
            }
            if (pt.getRawType() == List.class) {
                return makeList(toBuildClass, pt);
            }
            if (pt.getRawType() == Set.class) {
                return makeSet(toBuildClass, pt);
            }
            if (pt.getRawType() == EnumSet.class) {
                @SuppressWarnings("rawtypes")
                Enum enm = (Enum) makeArg(toBuildClass, pt.getActualTypeArguments()[0]);
                return EnumSet.of(enm);
            }
            if (pt.getRawType() == Supplier.class) {
                if (toBuildClass == AggExtractorInput.class) {
                    // AggValueInput just needs a valid java type in a supplier
                    Object o = randomBoolean() ? null : randomAlphaOfLength(5);
                    // But the supplier has to implement equals for randomValueOtherThan
                    return new Supplier<>() {
                        @Override
                        public Object get() {
                            return o;
                        }

                        @Override
                        public int hashCode() {
                            return Objects.hash(o);
                        }

                        @Override
                        public boolean equals(Object obj) {
                            if (obj == null || obj.getClass() != getClass()) {
                                return false;
                            }
                            Supplier<?> other = (Supplier<?>) obj;
                            return Objects.equals(o, other.get());
                        }
                    };
                }
            }
            Object obj = pluggableMakeParameterizedArg(toBuildClass, pt);
            if (obj != null) {
                return obj;
            }
            throw new IllegalArgumentException("Unsupported parameterized type [" + pt + "], for " + toBuildClass.getSimpleName());
        }
        if (argType instanceof WildcardType wt) {
            if (wt.getLowerBounds().length > 0 || wt.getUpperBounds().length > 1) {
                throw new IllegalArgumentException("Unsupported wildcard type [" + wt + "]");
            }
            return makeArg(toBuildClass, wt.getUpperBounds()[0]);
        }
        Class<?> argClass = (Class<?>) argType;

        /*
         * Sometimes all of the required type information isn't in the ctor
         * so we have to hard code it here.
         */
        if (toBuildClass == FieldAttribute.class) {
            // Prevent stack overflows by recursively using FieldAttribute's constructor that expects a parent;
            // `parent` is nullable.
            if (argClass == FieldAttribute.class && randomBoolean()) {
                return null;
            }
        } else if (toBuildClass == NodeTests.ChildrenAreAProperty.class) {
            /*
             * While any subclass of DummyFunction will do here we want to prevent
             * stack overflow so we use the one without children.
             */
            if (argClass == NodeTests.Dummy.class) {
                return makeNode(NodeTests.NoChildren.class);
            }
        } else if (FullTextPredicate.class.isAssignableFrom(toBuildClass)) {
            /*
             * FullTextPredicate analyzes its string arguments on
             * construction so they have to be valid.
             */
            if (argClass == String.class) {
                int size = between(0, 5);
                StringBuilder b = new StringBuilder();
                for (int i = 0; i < size; i++) {
                    if (i != 0) {
                        b.append(';');
                    }
                    b.append(randomAlphaOfLength(5)).append('=').append(randomAlphaOfLength(5));
                }
                return b.toString();
            }
        } else if (toBuildClass == Like.class) {

            if (argClass == LikePattern.class) {
                return new LikePattern(randomAlphaOfLength(16), randomFrom('\\', '|', '/', '`'));
            }

        } else {
            Object postProcess = pluggableMakeArg(toBuildClass, argClass);
            if (postProcess != null) {
                return postProcess;
            }
        }
        if (Expression.class == argClass) {
            /*
             * Rather than use any old subclass of expression lets
             * use a simple one. Without this we're very prone to
             * stackoverflow errors while building the tree.
             */
            return UnresolvedAttributeTests.randomUnresolvedAttribute();
        }
        if (EnrichPolicy.class == argClass) {
            List<String> enrichFields = randomSubsetOf(List.of("e1", "e2", "e3"));
            return new EnrichPolicy(randomFrom("match", "range"), null, List.of(), randomFrom("m1", "m2"), enrichFields);
        }

        if (Pipe.class == argClass) {
            /*
             * Similar to expressions, mock pipes to avoid
             * stackoverflow errors while building the tree.
             */
            return BinaryPipesTests.randomUnaryPipe();
        }

        if (Processor.class == argClass) {
            /*
             * Similar to expressions, mock pipes to avoid
             * stackoverflow errors while building the tree.
             */
            return new ConstantProcessor(randomAlphaOfLength(16));
        }

        if (Node.class.isAssignableFrom(argClass)) {
            /*
             * Rather than attempting to mock subclasses of node
             * and emulate them we just try and instantiate an
             * appropriate subclass
             */
            @SuppressWarnings("unchecked") // safe because this is the lowest possible bounds for Node
            Class<? extends Node<?>> asNodeSubclass = (Class<? extends Node<?>>) argType;
            return makeNode(asNodeSubclass);
        }

        if (argClass.isEnum()) {
            // Can't mock enums but luckily we can just pick one
            return randomFrom(argClass.getEnumConstants());
        }
        if (argClass == boolean.class) {
            // Can't mock primitives....
            return randomBoolean();
        }
        if (argClass == int.class) {
            return randomInt();
        }
        if (argClass == String.class) {
            // Nor strings
            return randomAlphaOfLength(5);
        }
        if (argClass == Source.class) {
            // Location is final and can't be mocked but we have a handy method to generate ones.
            return SourceTests.randomSource();
        }
        if (argClass == ZoneId.class) {
            // ZoneId is a sealed class (cannot be mocked) starting with Java 19
            return randomZone();
        }
        try {
            return mock(argClass);
        } catch (MockitoException e) {
            throw new RuntimeException("failed to mock [" + argClass.getName() + "] for [" + toBuildClass.getName() + "]", e);
        }
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
                var candidate = randomFrom(subclassesOf(asNodeSubclass, CLASSNAME_FILTER));
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
