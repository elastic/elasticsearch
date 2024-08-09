package org.elasticsearch.nalbind.injector;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nalbind.exceptions.CyclicDependencyException;
import org.elasticsearch.nalbind.api.Inject;
import org.elasticsearch.nalbind.exceptions.InjectionConfigurationException;
import org.elasticsearch.nalbind.injector.spec.AliasSpec;
import org.elasticsearch.nalbind.injector.spec.AmbiguousSpec;
import org.elasticsearch.nalbind.injector.spec.ExistingInstanceSpec;
import org.elasticsearch.nalbind.injector.spec.InjectionModifiers;
import org.elasticsearch.nalbind.injector.spec.InjectionSpec;
import org.elasticsearch.nalbind.injector.spec.MethodHandleSpec;
import org.elasticsearch.nalbind.injector.spec.ParameterSpec;
import org.elasticsearch.nalbind.injector.spec.UnambiguousSpec;
import org.elasticsearch.nalbind.injector.step.InjectionStep;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

/**
 * The main object for dependency injection.
 * <p>
 * Allows the user to specify the requirements, then call {@link #inject} to create an object plus all its dependencies.
 * <p>
 * <em>Implementation note</em>: this class itself contains logic for <em>specifying</em> the injection requirements;
 * the actual injection operations are performed in other classes like {@link Planner}, {@link PlanInterpreter}, and {@link ProxyPool}.
 */
public final class Injector {
    private final Set<Class<?>> classesToInstantiate;
    private final Map<Class<?>, Object> existingInstances;
    private final MethodHandles.Lookup lookup;
    private final ProxyPool proxyPool = new ProxyPool();

    Injector(Collection<Class<?>> classesToInstantiate, Map<Class<?>, Object> existingInstances, MethodHandles.Lookup lookup) {
        this.classesToInstantiate = new LinkedHashSet<>(classesToInstantiate);
        this.existingInstances = existingInstances;
        this.lookup = lookup;
    }

    public static Injector create(MethodHandles.Lookup lookup) {
        return new Injector(new LinkedHashSet<>(), new LinkedHashMap<>(), lookup);
    }

    /**
     * Instructs the injector to instantiate <code>classToProcess</code>
     * in accordance with whatever annotations may be present on that class.
     * <p>
     * There are only three ways the injector can find out that it must instantiate some class:
     * <ol>
     *     <li>
     *         This method (which is also used to implement {@link org.elasticsearch.nalbind.api.AutoInjectable @Autoinjectable})
     *     </li>
     *     <li>
     *         The parameter passed to {@link #inject}
     *     </li>
     *     <li>
     *         A constructor parameter of some other class being instantiated,
     *         having exactly the right class (not a supertype)
     *     </li>
     * </ol>
     *
     * @return <code>this</code>
     */
    public Injector addClass(Class<?> classToProcess) {
        this.classesToInstantiate.add(classToProcess);
        return this;
    }

    /**
     * Equivalent to multiple chained calls to {@link #addClass}.
     */
    public Injector addClasses(Class<?>... classesToProcess) {
        return addClasses(Arrays.asList(classesToProcess));
    }

    /**
     * Equivalent to multiple chained calls to {@link #addClass}.
     */
    public Injector addClasses(Collection<Class<?>> classesToProcess) {
        this.classesToInstantiate.addAll(classesToProcess);
        return this;
    }

    /**
     * Equivalent to {@link #addInstance addInstance(object.getClass(), object)}.
     */
    public <T> Injector addInstance(T object) {
        @SuppressWarnings("unchecked")
        Class<? super T> aClass = (Class<? super T>) object.getClass();
        return addInstance(aClass, object);
    }

    /**
     * Equivalent to multiple calls to {@link #addInstance(Object)}.
     */
    public Injector addInstances(Object... objects) {
        for (var x : objects) {
            addInstance(x);
        }
        return this;
    }

    /**
     * Equivalent to multiple calls to {@link #addInstance(Object)}.
     */
    public Injector addInstances(Iterable<?> objects) {
        for (var x : objects) {
            addInstance(x);
        }
        return this;
    }

    /**
     * Indicates that <code>object</code> is to be injected for parameters of type <code>type</code>.
     * The given object is treated as though it had been instantiated by the injector.
     */
    public <T> Injector addInstance(Class<? super T> type, T object) {
        Object existing = this.existingInstances.put(type, object);
        if (existing != null) {
            throw new IllegalStateException("There's already an object for " + type);
        }
        return this;
    }

    /**
     * For each "component" (getter) <em>c</em> of a {@link Record}, calls {@link #addInstance(Class, Object)} to register the
     * value with the component's declared type.
     */
    public <T> Injector addRecordContents(Record r) {
        for (var c: r.getClass().getRecordComponents()) {
            try {
                @SuppressWarnings("unchecked")
                Class<T> type = (Class<T>) c.getType();
                addInstance(type, type.cast(lookup.unreflect(c.getAccessor()).invoke(r)));
            } catch (Throwable e) {
                throw new InjectionConfigurationException("Unable to read record component " + c, e);
            }
        }
        return this;
    }

    public void inject() {
        doInjection();
    }

    /**
     * @param resultType The type of object to return.
     *                   Can't be a list; if you want a list, wrap it in a record.
     */
    public <T> T inject(Class<T> resultType) {
        ensureClassIsSpecified(resultType);
        return doInjection().theOnlyInstance(resultType);
    }

    /**
     * Like {@link #inject(Class)} but can return multiple result objects
     * @return {@link Map} whose keys are all the requested <code>resultTypes</code> and whose values are all the instances of those types.
     */
    public Map<Class<?>, List<?>> inject(Collection<? extends Class<?>> resultTypes) {
        resultTypes.forEach(this::ensureClassIsSpecified);
        PlanInterpreter i = doInjection();
        return resultTypes.stream()
            .collect(toMap(c->c, i::getInstances));
    }

    private <T> void ensureClassIsSpecified(Class<T> resultType) {
        if (false == (existingInstances.containsKey(resultType) || classesToInstantiate.contains(resultType))) {
            classesToInstantiate.add(resultType);
        }
    }

    private PlanInterpreter doInjection() {
        LOGGER.debug("Starting injection");
        Map<Class<?>, InjectionSpec> specMap = specMap(existingInstances, classesToInstantiate, lookup);
        PlanInterpreter interpreter = new PlanInterpreter(existingInstances, proxyPool);
        interpreter.executePlan(injectionPlan(classesToInstantiate, specMap));
        LOGGER.debug("Done injection");
        return interpreter;
    }

    /**
     * Finds an {@link InjectionSpec} for every class the injector is capable of injecting.
     * <p>
     * We do this once the injector is fully configured, with all calls to {@link #addClass} and {@link #addInstance} finished,
     * so that we can easily build the complete picture of how injection should occur.
     * <p>
     * This is not part of the planning process; it's just discovering all the things
     * the injector needs to know about. This logic isn't concerned with ordering, though it can detect dependency cycles.
     *
     * @return an {@link InjectionSpec} for every class the injector is capable of injecting.
     *
     * @param lookup is used to find {@link MethodHandle}s for constructors.
     */
    private static Map<Class<?>, InjectionSpec> specMap(
        Map<Class<?>, Object> existingInstances,
        Set<Class<?>> classesToInstantiate,
        MethodHandles.Lookup lookup
    ) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Classes to instantiate: {}", classesToInstantiate.stream().map(Class::getSimpleName).toList());
        }

        Queue<ParameterSpec> queue = classesToInstantiate.stream()
            .map(Injector::syntheticParameterSpec)
            .collect(toCollection(ArrayDeque::new));
        Map<Class<?>, InjectionSpec> specsByClass = new LinkedHashMap<>();
        existingInstances.forEach((type, obj) -> {
            registerSpec(new ExistingInstanceSpec(type, obj), specsByClass);
            aliasSuperinterfaces(type, type, specsByClass);
        });
        Set<Class<?>> checklist = new HashSet<>();

        ParameterSpec p;
        while ((p = queue.poll()) != null) {
            Class<?> c = p.injectableType();
            InjectionSpec existingResult = specsByClass.get(c);
            if (existingResult != null) {
                LOGGER.trace("Spec for {} already exists", c.getSimpleName());
                continue;
            }

            // At this point, we know we'll need to create a MethodHandleSpec

            if (checklist.add(c)) {
                Constructor<?> constructor = getSuitableConstructorIfAny(c);
                if (constructor == null) {
                    throw new InjectionConfigurationException("No suitable constructor for " + c);
                }

                LOGGER.trace("Inspecting parameters for constructor: {}", constructor);
                for (var parameter: constructor.getParameters()) {
                    ParameterSpec ps = ParameterSpec.from(parameter);
                    LOGGER.trace("Enqueue {}", parameter);
                    queue.add(ps);
                }

                MethodHandle ctorHandle;
                try {
                    ctorHandle = lookup.unreflectConstructor(constructor);
                } catch (IllegalAccessException e) {
                    // Uh oh. Try setAccessible
                    constructor.setAccessible(true);
                    try {
                        ctorHandle = lookup.unreflectConstructor(constructor);
                    } catch (IllegalAccessException ex) {
                        throw new InjectionConfigurationException(e);
                    }
                }

                List<ParameterSpec> parameters = Stream.of(constructor.getParameters())
                    .map(ParameterSpec::from)
                    .toList();

                registerSpec(
                    new MethodHandleSpec(constructor.getDeclaringClass(), ctorHandle, parameters),
                    specsByClass
                );
                aliasSuperinterfaces(c, c, specsByClass);
                for (Class<?> superclass = c.getSuperclass(); superclass != Object.class; superclass = superclass.getSuperclass()) {
                    if (Modifier.isAbstract(superclass.getModifiers())) {
                        registerSpec(new AliasSpec(superclass, c), specsByClass);
                    } else {
                        LOGGER.trace("Not aliasing {} to concrete superclass {}", c.getSimpleName(), superclass.getSimpleName());
                    }
                    aliasSuperinterfaces(superclass, c, specsByClass);
                }
            } else {
                throw new CyclicDependencyException("Cycle detected");
            }
        }
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Specs: {}", specsByClass.values().stream()
                .filter(s -> s instanceof UnambiguousSpec)
                .map(Object::toString)
                .collect(joining("\n\t", "\n\t", "")));
        }
        return specsByClass;
    }

    /**
     * For the classes we've been explicitly asked to inject,
     * pretend there's some massive method taking all of them as parameters
     */
    private static ParameterSpec syntheticParameterSpec(Class<?> c) {
        return new ParameterSpec("synthetic_" + c.getSimpleName(), c, c, InjectionModifiers.NONE);
    }

    private static Constructor<?> getSuitableConstructorIfAny(Class<?> type) {
        var constructors = Stream.of(type.getDeclaredConstructors()).filter(not(Constructor::isSynthetic)).toList();
        if (constructors.size() == 1) {
            return constructors.get(0);
        }
        var injectConstructors = constructors.stream().filter(c -> c.isAnnotationPresent(Inject.class)).toList();
        if (injectConstructors.size() == 1) {
            return injectConstructors.get(0);
        }
        LOGGER.trace("No suitable constructor for {}", type);
        return null;
    }

    /**
     * When creating <code>specsByClass</code>, we compute a kind of "inheritance closure"
     * in the sense that, for each class <code>C</code>, we not only add an entry for <code>C</code>,
     * but we also add {@link AliasSpec} entries for all abstract supertypes.
     * <p>
     * This method is part of the recursion that achieves this.
     */
    private static void aliasSuperinterfaces(Class<?> classToScan, Class<?> classToAlias, Map<Class<?>, InjectionSpec> specsByClass) {
        for (var i : classToScan.getInterfaces()) {
            registerSpec(new AliasSpec(i, classToAlias), specsByClass);
            aliasSuperinterfaces(i, classToAlias, specsByClass);
        }
    }

    private static void registerSpec(InjectionSpec spec, Map<Class<?>, InjectionSpec> specsByClass) {
        Class<?> requestedType = spec.requestedType();
        var existing = specsByClass.put(requestedType, spec);
        if (existing == null || existing.equals(spec)) {
            LOGGER.trace("Register spec: {}", spec);
        } else {
            AmbiguousSpec ambiguousSpec = new AmbiguousSpec(requestedType, spec, existing);
            LOGGER.trace("Ambiguity discovered for {}", requestedType);
            specsByClass.put(requestedType, ambiguousSpec);
        }
    }

    private List<InjectionStep> injectionPlan(Set<Class<?>> requiredClasses, Map<Class<?>, InjectionSpec> specsByClass) {
        // TODO: Cycle detection and reporting. Use SCCs
        LOGGER.trace("Constructing instantiation plan");
        Set<Class<?>> allParameterTypes = new HashSet<>();
        specsByClass.values().forEach(spec -> {
            if (spec instanceof MethodHandleSpec m) {
                m.parameters().stream()
                    .map(ParameterSpec::injectableType)
                    .forEachOrdered(allParameterTypes::add);
            }
        });

        var plan = new Planner(specsByClass, requiredClasses, allParameterTypes).injectionPlan();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Injection plan: {}", plan.stream().map(Object::toString).collect(joining("\n\t", "\n\t", "")));
        }
        return plan;
    }

    private static final Logger LOGGER = LogManager.getLogger(Injector.class);
}
