/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tree;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttributeTests;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.FullTextPredicate;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeSubclassTests;
import org.elasticsearch.xpack.esql.core.tree.NodeTests;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.mockito.exceptions.base.MockitoException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.mockito.Mockito.mock;

/**
 * Looks for all subclasses of {@link Node} and verifies that they
 * implement {@code Node.info} and
 * {@link Node#replaceChildren(List)} sanely. It'd be better if
 * each subclass had its own test case that verified those methods
 * and any other interesting things that they do, but we're a
 * long way from that and this gets the job done for now.
 * <p>
 * This test attempts to use reflection to create believable nodes
 * and manipulate them in believable ways with as little knowledge
 * of the actual subclasses as possible. This is problematic because
 * it is possible, for example, for nodes to stackoverflow because
 * they <strong>can</strong> contain themselves. So this class
 * <strong>does</strong> have some {@link Node}-subclass-specific
 * knowledge. As little as I could get away with though.
 * <p>
 * When there are actual tests for a subclass of {@linkplain Node}
 * then this class will do two things:
 * <ul>
 * <li>Skip running any tests for that subclass entirely.
 * <li>Delegate to that test to build nodes of that type when a
 * node of that type is called for.
 * </ul>
 */
public class EsqlNodeSubclassTests<T extends B, B extends Node<B>> extends NodeSubclassTests {
    private static final String ESQL_CORE_CLASS_PREFIX = "org.elasticsearch.xpack.esql.core";
    private static final String ESQL_CORE_JAR_LOCATION_SUBSTRING = "x-pack-esql-core";
    private static final String ESQL_CLASS_PREFIX = "org.elasticsearch.xpack.esql";

    private static final Predicate<String> CLASSNAME_FILTER = className -> {
        boolean esqlCore = className.startsWith(ESQL_CORE_CLASS_PREFIX) != false;
        boolean esqlProper = className.startsWith(ESQL_CLASS_PREFIX) != false;
        return (esqlCore || esqlProper);
    };

    /**
     * Scans the {@code .class} files to identify all classes and checks if
     * they are subclasses of {@link Node}.
     */
    @ParametersFactory(argumentFormatting = "%1s")
    @SuppressWarnings("rawtypes")
    public static List<Object[]> nodeSubclasses() throws IOException {
        return subclassesOf(Node.class, CLASSNAME_FILTER).stream()
            .filter(c -> testClassFor(c) == null)
            .map(c -> new Object[] { c })
            .toList();
    }

    private static final List<Class<?>> CLASSES_WITH_MIN_TWO_CHILDREN = List.of(Concat.class, CIDRMatch.class);

    // List of classes that are "unresolved" NamedExpression subclasses, therefore not suitable for use with logical/physical plan nodes.
    private static final List<Class<?>> UNRESOLVED_CLASSES = List.of(
        UnresolvedAttribute.class,
        UnresolvedException.class,
        UnresolvedFunction.class,
        UnresolvedNamedExpression.class
    );

    private final Class<T> subclass;

    public EsqlNodeSubclassTests(Class<T> subclass) {
        this.subclass = subclass;
    }

    public void testInfoParameters() throws Exception {
        Constructor<T> ctor = longestCtor(subclass);
        Object[] nodeCtorArgs = ctorArgs(ctor);
        T node = ctor.newInstance(nodeCtorArgs);
        /*
         * The count should be the same size as the longest constructor
         * by convention. If it isn't then we're missing something.
         */
        int expectedCount = ctor.getParameterCount();
        /*
         * Except the first `Location` argument of the ctor is implicit
         * in the parameters and not included.
         */
        expectedCount -= 1;

        assertEquals(expectedCount, info(node).properties().size());
    }

    /**
     * Test {@code Node.transformPropertiesOnly}
     * implementation on {@link #subclass} which tests the implementation of
     * {@code Node.info}. And tests the actual {@link NodeInfo} subclass
     * implementations in the process.
     */
    public void testTransform() throws Exception {
        if (FieldAttribute.class.equals(subclass)) {
            assumeTrue("FieldAttribute private constructor", false);
        }
        Constructor<T> ctor = longestCtor(subclass);
        Object[] nodeCtorArgs = ctorArgs(ctor);
        T node = ctor.newInstance(nodeCtorArgs);

        Type[] argTypes = ctor.getGenericParameterTypes();
        // start at 1 because we can't change Location.
        for (int changedArgOffset = 1; changedArgOffset < ctor.getParameterCount(); changedArgOffset++) {
            Object originalArgValue = nodeCtorArgs[changedArgOffset];

            Type changedArgType = argTypes[changedArgOffset];
            Object changedArgValue = randomValueOtherThanMaxTries(
                nodeCtorArgs[changedArgOffset],
                () -> makeArg(changedArgType),
                // JoinType has only 1 permitted enum element. Limit the number of retries.
                3
            );

            B transformed = transformNodeProps(node, Object.class, prop -> Objects.equals(prop, originalArgValue) ? changedArgValue : prop);

            if (node.children().contains(originalArgValue) || node.children().equals(originalArgValue)) {
                if (node.children().equals(emptyList()) && originalArgValue.equals(emptyList())) {
                    /*
                     * If the children are an empty list and the value
                     * we want to change is an empty list they'll be
                     * equal to one another so they'll come on this branch.
                     * This case is rare and hard to reason about so we're
                     * just going to assert nothing here and hope to catch
                     * it when we write non-reflection hack tests.
                     */
                    continue;
                }
                // Transformation shouldn't apply to children.
                assertSame(node, transformed);
            } else {
                assertTransformedOrReplacedChildren(node, transformed, ctor, nodeCtorArgs, changedArgOffset, changedArgValue);
            }
        }
    }

    /**
     * Test {@link Node#replaceChildren(List)} implementation on {@link #subclass}.
     */
    public void testReplaceChildren() throws Exception {
        Constructor<T> ctor = longestCtor(subclass);
        Object[] nodeCtorArgs = ctorArgs(ctor);
        T node = ctor.newInstance(nodeCtorArgs);

        Type[] argTypes = ctor.getGenericParameterTypes();
        // start at 1 because we can't change Location.
        for (int changedArgOffset = 1; changedArgOffset < ctor.getParameterCount(); changedArgOffset++) {
            Object originalArgValue = nodeCtorArgs[changedArgOffset];
            Type changedArgType = argTypes[changedArgOffset];

            if (originalArgValue instanceof Collection<?> col) {

                if (col.isEmpty() || col instanceof EnumSet) {
                    /*
                     * We skip empty lists here because they'll spuriously
                     * pass the conditions below if statements even if they don't
                     * have anything to do with children. This might cause us to
                     * ignore the case where a parameter gets copied into the
                     * children and just happens to be empty but I don't really
                     * know another way.
                     */

                    continue;
                }

                if (col instanceof List<?> originalList && node.children().equals(originalList)) {
                    // The arg we're looking at *is* the children
                    @SuppressWarnings("unchecked") // we pass a reasonable type so get reasonable results
                    List<B> newChildren = (List<B>) makeListOfSameSizeOtherThan(changedArgType, originalList);
                    B transformed = node.replaceChildren(newChildren);
                    assertTransformedOrReplacedChildren(node, transformed, ctor, nodeCtorArgs, changedArgOffset, newChildren);
                } else if (false == col.isEmpty() && node.children().containsAll(col)) {
                    // The arg we're looking at is a collection contained within the children
                    List<?> originalList = (List<?>) originalArgValue;

                    // First make the new children
                    @SuppressWarnings("unchecked") // we pass a reasonable type so get reasonable results
                    List<B> newCollection = (List<B>) makeListOfSameSizeOtherThan(changedArgType, originalList);

                    // Now merge that list of children into the original list of children
                    List<B> originalChildren = node.children();
                    List<B> newChildren = new ArrayList<>(originalChildren.size());
                    int originalOffset = 0;
                    for (int i = 0; i < originalChildren.size(); i++) {
                        if (originalOffset < originalList.size() && originalChildren.get(i).equals(originalList.get(originalOffset))) {
                            newChildren.add(newCollection.get(originalOffset));
                            originalOffset++;
                        } else {
                            newChildren.add(originalChildren.get(i));
                        }
                    }

                    // Finally! We can assert.....
                    B transformed = node.replaceChildren(newChildren);
                    assertTransformedOrReplacedChildren(node, transformed, ctor, nodeCtorArgs, changedArgOffset, newCollection);
                } else {
                    // The arg we're looking at has nothing to do with the children
                }
            } else {
                if (node.children().contains(originalArgValue)) {
                    // The arg we're looking at is one of the children
                    List<B> newChildren = new ArrayList<>(node.children());
                    @SuppressWarnings("unchecked") // makeArg produced reasonable values
                    B newChild = (B) randomValueOtherThan(nodeCtorArgs[changedArgOffset], () -> makeArg(changedArgType));
                    newChildren.replaceAll(e -> Objects.equals(originalArgValue, e) ? newChild : e);
                    B transformed = node.replaceChildren(newChildren);
                    assertTransformedOrReplacedChildren(node, transformed, ctor, nodeCtorArgs, changedArgOffset, newChild);
                } else {
                    // The arg we're looking at has nothing to do with the children
                }
            }
        }
    }

    /**
     * Build a list of arguments to use when calling
     * {@code ctor} that make sense when {@code ctor}
     * builds subclasses of {@link Node}.
     */
    private Object[] ctorArgs(Constructor<? extends Node<?>> ctor) throws Exception {
        Type[] argTypes = ctor.getGenericParameterTypes();
        Object[] args = new Object[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            final int currentArgIndex = i;
            args[i] = randomValueOtherThanMany(candidate -> {
                for (int a = 0; a < currentArgIndex; a++) {
                    if (Objects.equals(args[a], candidate)) {
                        return true;
                    }
                }
                return false;
            }, () -> {
                try {
                    return makeArg(ctor.getDeclaringClass(), argTypes[currentArgIndex]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return args;
    }

    /**
     * Make an argument to feed the {@link #subclass}'s ctor.
     */
    protected Object makeArg(Type argType) {
        try {
            return makeArg(subclass, argType);
        } catch (Exception e) {
            // Wrap to make `randomValueOtherThan` happy.
            throw new RuntimeException(e);
        }
    }

    /**
     * Make an argument to feed to the constructor for {@code toBuildClass}.
     */
    @SuppressWarnings("unchecked")
    private Object makeArg(Class<? extends Node<?>> toBuildClass, Type argType) throws Exception {

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
        } else if (argClass == Dissect.Parser.class) {
            // Dissect.Parser is a record / final, cannot be mocked
            String pattern = randomDissectPattern();
            String appendSeparator = randomAlphaOfLength(16);
            return new Dissect.Parser(pattern, appendSeparator, new DissectParser(pattern, appendSeparator));
        } else if (argClass == Grok.Parser.class) {
            // Grok.Parser is a record / final, cannot be mocked
            return Grok.pattern(Source.EMPTY, randomGrokPattern());
        } else if (argClass == EsQueryExec.FieldSort.class) {
            // TODO: It appears neither FieldSort nor GeoDistanceSort are ever actually tested
            return randomFieldSort();
        } else if (argClass == EsQueryExec.GeoDistanceSort.class) {
            // TODO: It appears neither FieldSort nor GeoDistanceSort are ever actually tested
            return randomGeoDistanceSort();
        } else if (toBuildClass == Pow.class && Expression.class.isAssignableFrom(argClass)) {
            return randomResolvedExpression(randomBoolean() ? FieldAttribute.class : Literal.class);
        } else if (isPlanNodeClass(toBuildClass) && Expression.class.isAssignableFrom(argClass)) {
            return randomResolvedExpression(argClass);
        } else if (argClass == Stat.class) {
            // record field
            return new Stat(randomRealisticUnicodeOfLength(10), randomFrom(StatsType.values()), null);
        } else if (argClass == Integer.class) {
            return randomInt();
        } else if (argClass == JoinType.class) {
            return JoinTypes.LEFT;
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
        if (argClass == Configuration.class) {
            return randomConfiguration();
        }
        if (argClass == JoinConfig.class) {
            return new JoinConfig(
                JoinTypes.LEFT,
                List.of(UnresolvedAttributeTests.randomUnresolvedAttribute()),
                List.of(UnresolvedAttributeTests.randomUnresolvedAttribute()),
                List.of(UnresolvedAttributeTests.randomUnresolvedAttribute())
            );
        }

        try {
            return mock(argClass);
        } catch (MockitoException e) {
            throw new RuntimeException("failed to mock [" + argClass.getName() + "] for [" + toBuildClass.getName() + "]", e);
        }
    }

    private List<?> makeList(Class<? extends Node<?>> toBuildClass, ParameterizedType listType) throws Exception {
        return makeList(toBuildClass, listType, randomSizeForCollection(toBuildClass));
    }

    private List<?> makeList(Class<? extends Node<?>> toBuildClass, ParameterizedType listType, int size) throws Exception {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(makeArg(toBuildClass, listType.getActualTypeArguments()[0]));
        }
        return list;
    }

    private Set<?> makeSet(Class<? extends Node<?>> toBuildClass, ParameterizedType listType) throws Exception {
        return makeSet(toBuildClass, listType, randomSizeForCollection(toBuildClass));
    }

    private Set<?> makeSet(Class<? extends Node<?>> toBuildClass, ParameterizedType listType, int size) throws Exception {
        Set<Object> list = new HashSet<>();
        for (int i = 0; i < size; i++) {
            list.add(makeArg(toBuildClass, listType.getActualTypeArguments()[0]));
        }
        return list;
    }

    private Object makeMap(Class<? extends Node<?>> toBuildClass, ParameterizedType pt) throws Exception {
        Map<Object, Object> map = new HashMap<>();
        int size = randomSizeForCollection(toBuildClass);
        while (map.size() < size) {
            Object key = makeArg(toBuildClass, pt.getActualTypeArguments()[0]);
            Object value = makeArg(toBuildClass, pt.getActualTypeArguments()[1]);
            map.put(key, value);
        }
        return map;
    }

    private int randomSizeForCollection(Class<? extends Node<?>> toBuildClass) {
        int minCollectionLength = 0;
        int maxCollectionLength = 10;

        if (hasAtLeastTwoChildren(toBuildClass)) {
            minCollectionLength = 2;
        }
        return between(minCollectionLength, maxCollectionLength);
    }

    private List<?> makeListOfSameSizeOtherThan(Type listType, List<?> original) throws Exception {
        if (original.isEmpty()) {
            throw new IllegalArgumentException("Can't make a different empty list");
        }
        return randomValueOtherThan(original, () -> {
            try {
                return makeList(subclass, (ParameterizedType) listType, original.size());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

    }

    public <T extends Node<?>> T makeNode(Class<? extends T> nodeClass) throws Exception {
        if (Modifier.isAbstract(nodeClass.getModifiers())) {
            nodeClass = randomFrom(innerSubclassesOf(nodeClass));
        }
        Class<?> testSubclassFor = testClassFor(nodeClass);
        if (testSubclassFor != null) {
            // Delegate to the test class for a node if there is one
            Method m = testSubclassFor.getMethod("random" + Strings.capitalize(nodeClass.getSimpleName()));
            assert Modifier.isStatic(m.getModifiers()) : "Expected static method, got:" + m;
            return nodeClass.cast(m.invoke(null));
        }
        Constructor<? extends T> ctor = longestCtor(nodeClass);
        Object[] nodeCtorArgs = ctorArgs(ctor);
        return ctor.newInstance(nodeCtorArgs);
    }

    private void assertTransformedOrReplacedChildren(
        T node,
        B transformed,
        Constructor<T> ctor,
        Object[] nodeCtorArgs,
        int changedArgOffset,
        Object changedArgValue
    ) throws Exception {
        if (node instanceof Function) {
            /*
             * Functions have a weaker definition of transform then other
             * things:
             *
             * Transforming using the way we did above should only change
             * the one property of the node that we intended to transform.
             */
            assertEquals(node.source(), transformed.source());
            List<Object> op = node.nodeProperties();
            List<Object> tp = transformed.nodeProperties();
            for (int p = 0; p < op.size(); p++) {
                if (p == changedArgOffset - 1) { // -1 because location isn't in the list
                    assertEquals(changedArgValue, tp.get(p));
                } else {
                    assertEquals(op.get(p), tp.get(p));
                }
            }
        } else {
            /*
             * The stronger assertion for all non-Functions: transforming
             * a node changes *only* the transformed value such that you
             * can rebuild a copy of the node using its constructor changing
             * only one argument and it'll be *equal* to the result of the
             * transformation.
             */
            Type[] argTypes = ctor.getGenericParameterTypes();
            Object[] args = new Object[argTypes.length];
            for (int i = 0; i < argTypes.length; i++) {
                args[i] = nodeCtorArgs[i] == nodeCtorArgs[changedArgOffset] ? changedArgValue : nodeCtorArgs[i];
            }
            T reflectionTransformed = ctor.newInstance(args);
            assertEquals(reflectionTransformed, transformed);
        }
    }

    /**
     * Find the longest constructor of the given class.
     * By convention, for all subclasses of {@link Node},
     * this constructor should have "all" of the state of
     * the node. All other constructors should all delegate
     * to this constructor.
     */
    static <T> Constructor<T> longestCtor(Class<T> clazz) {
        Constructor<T> longest = null;
        for (Constructor<?> ctor : clazz.getConstructors()) {
            if (longest == null || longest.getParameterCount() < ctor.getParameterCount()) {
                @SuppressWarnings("unchecked") // Safe because the ctor has to be a ctor for T
                Constructor<T> castCtor = (Constructor<T>) ctor;
                longest = castCtor;
            }
        }
        if (longest == null) {
            throw new IllegalArgumentException("Couldn't find any constructors for [" + clazz.getName() + "]");
        }
        return longest;
    }

    private boolean hasAtLeastTwoChildren(Class<? extends Node<?>> toBuildClass) {
        return CLASSES_WITH_MIN_TWO_CHILDREN.stream().anyMatch(toBuildClass::equals);
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
                if (UNRESOLVED_CLASSES.stream().allMatch(unresolved -> unresolved.isAssignableFrom(candidate) == false)) {
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

    public static String randomGrokPattern() {
        return randomFrom(
            Set.of("%{NUMBER:b:int} %{NUMBER:c:float} %{NUMBER:d:double} %{WORD:e:boolean}", "[a-zA-Z0-9._-]+", "%{LOGLEVEL}")
        );
    }

    static List<DataType> DATA_TYPES = DataType.types()
        .stream()
        .filter(d -> DataType.UNDER_CONSTRUCTION.containsKey(d) == false || Build.current().isSnapshot())
        .toList();

    static EsQueryExec.FieldSort randomFieldSort() {
        return new EsQueryExec.FieldSort(
            field(randomAlphaOfLength(16), randomFrom(DATA_TYPES)),
            randomFrom(EnumSet.allOf(Order.OrderDirection.class)),
            randomFrom(EnumSet.allOf(Order.NullsPosition.class))
        );
    }

    static EsQueryExec.GeoDistanceSort randomGeoDistanceSort() {
        return new EsQueryExec.GeoDistanceSort(
            field(randomAlphaOfLength(16), GEO_POINT),
            randomFrom(EnumSet.allOf(Order.OrderDirection.class)),
            randomDoubleBetween(-90, 90, false),
            randomDoubleBetween(-180, 180, false)
        );
    }

    static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Collections.emptyMap(), false));
    }

    public static <T> Set<Class<? extends T>> subclassesOf(Class<T> clazz) throws IOException {
        return subclassesOf(clazz, CLASSNAME_FILTER);
    }

    private <T> Set<Class<? extends T>> innerSubclassesOf(Class<T> clazz) throws IOException {
        return subclassesOf(clazz, CLASSNAME_FILTER);
    }

    /**
     * Cache of subclasses. We use a cache because it significantly speeds up
     * the test.
     */
    private static final Map<Class<?>, Set<?>> subclassCache = new HashMap<>();

    /**
     * Find all subclasses of a particular class.
     */
    public static <T> Set<Class<? extends T>> subclassesOf(Class<T> clazz, Predicate<String> classNameFilter) throws IOException {
        @SuppressWarnings("unchecked") // The map is built this way
        Set<Class<? extends T>> lookup = (Set<Class<? extends T>>) subclassCache.get(clazz);
        if (lookup != null) {
            return lookup;
        }
        Set<Class<? extends T>> results = new LinkedHashSet<>();
        String[] paths = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        for (String path : paths) {
            Path root = PathUtils.get(path);
            int rootLength = root.toString().length() + 1;

            // load classes from jar files
            // NIO FileSystem API is not used since it trips the SecurityManager
            // https://bugs.openjdk.java.net/browse/JDK-8160798
            // so iterate the jar "by hand"
            if (path.endsWith(".jar") && path.contains(ESQL_CORE_JAR_LOCATION_SUBSTRING)) {
                try (JarInputStream jar = jarStream(root)) {
                    JarEntry je = null;
                    while ((je = jar.getNextJarEntry()) != null) {
                        String name = je.getName();
                        if (name.endsWith(".class")) {
                            String className = name.substring(0, name.length() - ".class".length()).replace("/", ".");
                            maybeLoadClass(clazz, className, root + "!/" + name, classNameFilter, results);
                        }
                    }
                }
            }
            // for folders, just use the FileSystems API
            else {
                Files.walkFileTree(root, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (Files.isRegularFile(file) && file.getFileName().toString().endsWith(".class")) {
                            String fileName = file.toString();
                            // Chop off the root and file extension
                            String className = fileName.substring(rootLength, fileName.length() - ".class".length());
                            // Go from "path" style to class style
                            className = className.replace(PathUtils.getDefaultFileSystem().getSeparator(), ".");
                            maybeLoadClass(clazz, className, fileName, classNameFilter, results);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
        subclassCache.put(clazz, results);
        return results;
    }

    @SuppressForbidden(reason = "test reads from jar")
    private static JarInputStream jarStream(Path path) throws IOException {
        return new JarInputStream(path.toUri().toURL().openStream());
    }

    /**
     * Load classes from predefined packages (hack to limit the scope) and if they match the hierarchy, add them to the cache
     */
    private static <T> void maybeLoadClass(
        Class<T> clazz,
        String className,
        String location,
        Predicate<String> classNameFilter,
        Set<Class<? extends T>> results
    ) throws IOException {
        if (classNameFilter.test(className) == false) {
            return;
        }

        Class<?> c;
        try {
            c = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IOException("Couldn't load " + location, e);
        }

        if (false == Modifier.isAbstract(c.getModifiers()) && false == c.isAnonymousClass() && clazz.isAssignableFrom(c)) {
            Class<? extends T> s = c.asSubclass(clazz);
            results.add(s);
        }
    }

    /**
     * The test class for some subclass of node or {@code null}
     * if there isn't such a class or it doesn't extend
     * {@link AbstractNodeTestCase}.
     */
    protected static Class<?> testClassFor(Class<?> nodeSubclass) {
        String testClassName = nodeSubclass.getName() + "Tests";
        try {
            Class<?> c = Class.forName(testClassName);
            if (AbstractNodeTestCase.class.isAssignableFrom(c)) {
                return c;
            }
            return null;
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static <T> T randomValueOtherThanManyMaxTries(Predicate<T> input, Supplier<T> randomSupplier, int maxTries) {
        int[] maxTriesHolder = { maxTries };
        Predicate<T> inputWithMaxTries = t -> input.test(t) && maxTriesHolder[0]-- > 0;

        return ESTestCase.randomValueOtherThanMany(inputWithMaxTries, randomSupplier);
    }

    public static <T> T randomValueOtherThanMaxTries(T input, Supplier<T> randomSupplier, int maxTries) {
        return randomValueOtherThanManyMaxTries(v -> Objects.equals(input, v), randomSupplier, maxTries);
    }
}
