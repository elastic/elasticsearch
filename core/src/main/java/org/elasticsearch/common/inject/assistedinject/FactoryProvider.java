/**
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.assistedinject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.HasDependencies;
import org.elasticsearch.common.inject.spi.Message;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides a factory that combines the caller's arguments with injector-supplied values to
 * construct objects.
 * <p/>
 * <h3>Defining a factory</h3>
 * Create an interface whose methods return the constructed type, or any of its supertypes. The
 * method's parameters are the arguments required to build the constructed type.
 * <pre>public interface PaymentFactory {
 *   Payment create(Date startDate, Money amount);
 * }</pre>
 * You can name your factory methods whatever you like, such as <i>create</i>, <i>createPayment</i>
 * or <i>newPayment</i>.
 * <p/>
 * <h3>Creating a type that accepts factory parameters</h3>
 * {@code constructedType} is a concrete class with an {@literal @}{@link Inject}-annotated
 * constructor. In addition to injector-supplied parameters, the constructor should have
 * parameters that match each of the factory method's parameters. Each factory-supplied parameter
 * requires an {@literal @}{@link Assisted} annotation. This serves to document that the parameter
 * is not bound by your application's modules.
 * <pre>public class RealPayment implements Payment {
 *   {@literal @}Inject
 *   public RealPayment(
 *      CreditService creditService,
 *      AuthService authService,
 *      <strong>{@literal @}Assisted Date startDate</strong>,
 *      <strong>{@literal @}Assisted Money amount</strong>) {
 *     ...
 *   }
 * }</pre>
 * Any parameter that permits a null value should also be annotated {@code @Nullable}.
 * <p/>
 * <h3>Configuring factories</h3>
 * In your {@link org.elasticsearch.common.inject.Module module}, bind the factory interface to the returned
 * factory:
 * <pre>bind(PaymentFactory.class).toProvider(
 *     FactoryProvider.newFactory(PaymentFactory.class, RealPayment.class));</pre>
 * As a side-effect of this binding, Guice will inject the factory to initialize it for use. The
 * factory cannot be used until the injector has been initialized.
 * <p/>
 * <h3>Using the factory</h3>
 * Inject your factory into your application classes. When you use the factory, your arguments
 * will be combined with values from the injector to construct an instance.
 * <pre>public class PaymentAction {
 *   {@literal @}Inject private PaymentFactory paymentFactory;
 * <p/>
 *   public void doPayment(Money amount) {
 *     Payment payment = paymentFactory.create(new Date(), amount);
 *     payment.apply();
 *   }
 * }</pre>
 * <p/>
 * <h3>Making parameter types distinct</h3>
 * The types of the factory method's parameters must be distinct. To use multiple parameters of
 * the same type, use a named {@literal @}{@link Assisted} annotation to disambiguate the
 * parameters. The names must be applied to the factory method's parameters:
 * <p/>
 * <pre>public interface PaymentFactory {
 *   Payment create(
 *       <strong>{@literal @}Assisted("startDate")</strong> Date startDate,
 *       <strong>{@literal @}Assisted("dueDate")</strong> Date dueDate,
 *       Money amount);
 * } </pre>
 * ...and to the concrete type's constructor parameters:
 * <pre>public class RealPayment implements Payment {
 *   {@literal @}Inject
 *   public RealPayment(
 *      CreditService creditService,
 *      AuthService authService,
 *      <strong>{@literal @}Assisted("startDate")</strong> Date startDate,
 *      <strong>{@literal @}Assisted("dueDate")</strong> Date dueDate,
 *      <strong>{@literal @}Assisted</strong> Money amount) {
 *     ...
 *   }
 * }</pre>
 * <p/>
 * <h3>Values are created by Guice</h3>
 * Returned factories use child injectors to create values. The values are eligible for method
 * interception. In addition, {@literal @}{@literal Inject} members will be injected before they are
 * returned.
 * <p/>
 * <h3>Backwards compatibility using {@literal @}AssistedInject</h3>
 * Instead of the {@literal @}Inject annotation, you may annotate the constructed classes with
 * {@literal @}{@link AssistedInject}. This triggers a limited backwards-compatibility mode.
 * <p/>
 * <p>Instead of matching factory method arguments to constructor parameters using their names, the
 * <strong>parameters are matched by their order</strong>. The first factory method argument is
 * used for the first {@literal @}Assisted constructor parameter, etc.. Annotation names have no
 * effect.
 * <p/>
 * <p>Returned values are <strong>not created by Guice</strong>. These types are not eligible for
 * method interception. They do receive post-construction member injection.
 *
 * @param <F> The factory interface
 * @author jmourits@google.com (Jerome Mourits)
 * @author jessewilson@google.com (Jesse Wilson)
 * @author dtm@google.com (Daniel Martin)
 */
public class FactoryProvider<F> implements Provider<F>, HasDependencies {

    /*
    * This class implements the old @AssistedInject implementation that manually matches constructors
    * to factory methods. The new child injector implementation lives in FactoryProvider2.
    */

    private Injector injector;

    private final TypeLiteral<F> factoryType;
    private final Map<Method, AssistedConstructor<?>> factoryMethodToConstructor;

    public static <F> Provider<F> newFactory(
            Class<F> factoryType, Class<?> implementationType) {
        return newFactory(TypeLiteral.get(factoryType), TypeLiteral.get(implementationType));
    }

    public static <F> Provider<F> newFactory(
            TypeLiteral<F> factoryType, TypeLiteral<?> implementationType) {
        Map<Method, AssistedConstructor<?>> factoryMethodToConstructor
                = createMethodMapping(factoryType, implementationType);

        if (!factoryMethodToConstructor.isEmpty()) {
            return new FactoryProvider<>(factoryType, factoryMethodToConstructor);
        } else {
            return new FactoryProvider2<>(factoryType, Key.get(implementationType));
        }
    }

    private FactoryProvider(TypeLiteral<F> factoryType,
                            Map<Method, AssistedConstructor<?>> factoryMethodToConstructor) {
        this.factoryType = factoryType;
        this.factoryMethodToConstructor = factoryMethodToConstructor;
        checkDeclaredExceptionsMatch();
    }

    @Inject
    void setInjectorAndCheckUnboundParametersAreInjectable(Injector injector) {
        this.injector = injector;
        for (AssistedConstructor<?> c : factoryMethodToConstructor.values()) {
            for (Parameter p : c.getAllParameters()) {
                if (!p.isProvidedByFactory() && !paramCanBeInjected(p, injector)) {
                    // this is lame - we're not using the proper mechanism to add an
                    // error to the injector. Throughout this class we throw exceptions
                    // to add errors, which isn't really the best way in Guice
                    throw newConfigurationException("Parameter of type '%s' is not injectable or annotated "
                            + "with @Assisted for Constructor '%s'", p, c);
                }
            }
        }
    }

    private void checkDeclaredExceptionsMatch() {
        for (Map.Entry<Method, AssistedConstructor<?>> entry : factoryMethodToConstructor.entrySet()) {
            for (Class<?> constructorException : entry.getValue().getDeclaredExceptions()) {
                if (!isConstructorExceptionCompatibleWithFactoryExeception(
                        constructorException, entry.getKey().getExceptionTypes())) {
                    throw newConfigurationException("Constructor %s declares an exception, but no compatible "
                            + "exception is thrown by the factory method %s", entry.getValue(), entry.getKey());
                }
            }
        }
    }

    private boolean isConstructorExceptionCompatibleWithFactoryExeception(
            Class<?> constructorException, Class<?>[] factoryExceptions) {
        for (Class<?> factoryException : factoryExceptions) {
            if (factoryException.isAssignableFrom(constructorException)) {
                return true;
            }
        }
        return false;
    }

    private boolean paramCanBeInjected(Parameter parameter, Injector injector) {
        return parameter.isBound(injector);
    }

    private static Map<Method, AssistedConstructor<?>> createMethodMapping(
            TypeLiteral<?> factoryType, TypeLiteral<?> implementationType) {
        List<AssistedConstructor<?>> constructors = Lists.newArrayList();

        for (Constructor<?> constructor : implementationType.getRawType().getDeclaredConstructors()) {
            if (constructor.getAnnotation(AssistedInject.class) != null) {
                @SuppressWarnings("unchecked") // the constructor type and implementation type agree
                        AssistedConstructor assistedConstructor = new AssistedConstructor(
                        constructor, implementationType.getParameterTypes(constructor));
                constructors.add(assistedConstructor);
            }
        }

        if (constructors.isEmpty()) {
            return ImmutableMap.of();
        }

        Method[] factoryMethods = factoryType.getRawType().getMethods();

        if (constructors.size() != factoryMethods.length) {
            throw newConfigurationException("Constructor mismatch: %s has %s @AssistedInject "
                    + "constructors, factory %s has %s creation methods", implementationType,
                    constructors.size(), factoryType, factoryMethods.length);
        }

        Map<ParameterListKey, AssistedConstructor> paramsToConstructor = Maps.newHashMap();

        for (AssistedConstructor c : constructors) {
            if (paramsToConstructor.containsKey(c.getAssistedParameters())) {
                throw new RuntimeException("Duplicate constructor, " + c);
            }
            paramsToConstructor.put(c.getAssistedParameters(), c);
        }

        Map<Method, AssistedConstructor<?>> result = Maps.newHashMap();
        for (Method method : factoryMethods) {
            if (!method.getReturnType().isAssignableFrom(implementationType.getRawType())) {
                throw newConfigurationException("Return type of method %s is not assignable from %s",
                        method, implementationType);
            }

            List<Type> parameterTypes = Lists.newArrayList();
            for (TypeLiteral<?> parameterType : factoryType.getParameterTypes(method)) {
                parameterTypes.add(parameterType.getType());
            }
            ParameterListKey methodParams = new ParameterListKey(parameterTypes);

            if (!paramsToConstructor.containsKey(methodParams)) {
                throw newConfigurationException("%s has no @AssistInject constructor that takes the "
                        + "@Assisted parameters %s in that order. @AssistInject constructors are %s",
                        implementationType, methodParams, paramsToConstructor.values());
            }

            method.getParameterAnnotations();
            for (Annotation[] parameterAnnotations : method.getParameterAnnotations()) {
                for (Annotation parameterAnnotation : parameterAnnotations) {
                    if (parameterAnnotation.annotationType() == Assisted.class) {
                        throw newConfigurationException("Factory method %s has an @Assisted parameter, which "
                                + "is incompatible with the deprecated @AssistedInject annotation. Please replace "
                                + "@AssistedInject with @Inject on the %s constructor.",
                                method, implementationType);
                    }
                }
            }

            AssistedConstructor matchingConstructor = paramsToConstructor.remove(methodParams);

            result.put(method, matchingConstructor);
        }
        return result;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        List<Dependency<?>> dependencies = Lists.newArrayList();
        for (AssistedConstructor<?> constructor : factoryMethodToConstructor.values()) {
            for (Parameter parameter : constructor.getAllParameters()) {
                if (!parameter.isProvidedByFactory()) {
                    dependencies.add(Dependency.get(parameter.getPrimaryBindingKey()));
                }
            }
        }
        return ImmutableSet.copyOf(dependencies);
    }

    @Override
    public F get() {
        InvocationHandler invocationHandler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] creationArgs) throws Throwable {
                // pass methods from Object.class to the proxy
                if (method.getDeclaringClass().equals(Object.class)) {
                    return method.invoke(this, creationArgs);
                }

                AssistedConstructor<?> constructor = factoryMethodToConstructor.get(method);
                Object[] constructorArgs = gatherArgsForConstructor(constructor, creationArgs);
                Object objectToReturn = constructor.newInstance(constructorArgs);
                injector.injectMembers(objectToReturn);
                return objectToReturn;
            }

            public Object[] gatherArgsForConstructor(
                    AssistedConstructor<?> constructor,
                    Object[] factoryArgs) {
                int numParams = constructor.getAllParameters().size();
                int argPosition = 0;
                Object[] result = new Object[numParams];

                for (int i = 0; i < numParams; i++) {
                    Parameter parameter = constructor.getAllParameters().get(i);
                    if (parameter.isProvidedByFactory()) {
                        result[i] = factoryArgs[argPosition];
                        argPosition++;
                    } else {
                        result[i] = parameter.getValue(injector);
                    }
                }
                return result;
            }
        };

        @SuppressWarnings("unchecked") // we imprecisely treat the class literal of T as a Class<T>
                Class<F> factoryRawType = (Class) factoryType.getRawType();
        return factoryRawType.cast(Proxy.newProxyInstance(factoryRawType.getClassLoader(),
                new Class[]{factoryRawType}, invocationHandler));
    }

    private static ConfigurationException newConfigurationException(String format, Object... args) {
        return new ConfigurationException(ImmutableSet.of(new Message(Errors.format(format, args))));
    }
}
