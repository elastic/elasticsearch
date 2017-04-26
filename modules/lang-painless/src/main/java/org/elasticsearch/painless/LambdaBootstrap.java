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

package org.elasticsearch.painless;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.invoke.MethodHandles.Lookup;
import static org.elasticsearch.painless.Compiler.Loader;
import static org.elasticsearch.painless.WriterConstants.CLASS_VERSION;
import static org.elasticsearch.painless.WriterConstants.DELEGATE_BOOTSTRAP_HANDLE;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.H_INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.H_INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;

/**
 * LambdaBootstrap is used to generate all the code necessary to execute
 * lambda functions and method references within Painless.  The code generation
 * used here is based upon the following article:
 * http://cr.openjdk.java.net/~briangoetz/lambda/lambda-translation.html
 * However, it is a simplified version as Painless has no concept of generics
 * or serialization.  LambdaBootstrap is being used as a replacement for
 * {@link java.lang.invoke.LambdaMetafactory} since the Painless casting model
 * cannot be fully supported through this class.
 *
 * For each lambda function/method reference used within a Painless script
 * a class will be generated at link-time using the
 * {@link LambdaBootstrap#lambdaBootstrap} method that contains the following:
 * 1. member fields for any captured variables
 * 2. a constructor that will take in captured variables and assign them to
 * their respective member fields
 * 3. if there are captures, a factory method that will take in captured
 * variables and delegate them to the constructor
 * 4. a method that will load the member fields representing captured variables
 * and take in any other necessary values based on the arguments passed into the
 * lambda function/reference method; it will then make a delegated call to the
 * actual lambda function/reference method
 *
 * Take for example the following Painless script:
 *
 * {@code
 * List list1 = new ArrayList(); "
 * list1.add(2); "
 * List list2 = new ArrayList(); "
 * list1.forEach(x -> list2.add(x));"
 * return list[0]"
 * }
 *
 * The script contains a lambda function with a captured variable.
 * The following Lambda class would be generated:
 *
 * {@code
 *     public static final class $$Lambda0 implements Consumer {
 *         private List arg$0;
 *
 *         public $$Lambda0(List arg$0) {
 *             this.arg$0 = arg$0;
 *         }
 *
 *         public static $$Lambda0 get$Lambda(List arg$0) {
 *             return $$Lambda0(arg$0);
 *         }
 *
 *         public void accept(Object val$0) {
 *             Painless$Script.lambda$0(this.arg$0, val$0);
 *         }
 *     }
 *
 *     public class Painless$Script implements ... {
 *         ...
 *         public static lambda$0(List list2, Object x) {
 *             list2.add(x);
 *         }
 *         ...
 *     }
 * }
 *
 * Note if the above didn't have a captured variable then
 * the factory method get$Lambda would not have been generated.
 * Also the accept method actually uses an invokedynamic
 * instruction to call the lambda$0 method so that
 * {@link MethodHandle#asType} can be used to do the necessary
 * conversions between argument types without having to hard
 * code them.
 *
 * When the {@link CallSite} is linked the linked method depends
 * on whether or not there are captures.  If there are no captures
 * the same instance of the generated lambda class will be
 * returned each time by the factory method as there are no
 * changing values other than the arguments.  If there are
 * captures a new instance of the generated lambda class will
 * be returned each time with the captures passed into the
 * factory method to be stored in the member fields.
 */
public final class LambdaBootstrap {

    /**
     * Metadata for a captured variable used during code generation.
     */
    private static final class Capture {
        private final String name;
        private final Type type;
        private final String desc;

        /**
         * Converts incoming parameters into the name, type, and
         * descriptor for the captured argument.
         * @param count The captured argument count
         * @param type The class type of the captured argument
         */
        private Capture(int count, Class<?> type) {
            this.name = "arg$" + count;
            this.type = Type.getType(type);
            this.desc = this.type.getDescriptor();
        }
    }

    /**
     * A counter used to generate a unique name
     * for each lambda function/reference class.
     */
    private static final AtomicLong COUNTER = new AtomicLong(0);

    /**
     * Generates a lambda class for a lambda function/method reference
     * within a Painless script.  Variables with the prefix interface are considered
     * to represent values for code generated for the lambda class. Variables with
     * the prefix delegate are considered to represent values for code generated
     * within the Painless script.  The interface method delegates (calls) to the
     * delegate method.
     * @param lookup Standard {@link MethodHandles#lookup}
     * @param interfaceMethodName Name of functional interface method that is called
     * @param factoryMethodType The type of method to be linked to this CallSite; note that
     *                          captured types are based on the parameters for this method
     * @param interfaceMethodType The type of method representing the functional interface method
     * @param delegateClassName The name of the Painless script class
     * @param delegateInvokeType The type of method call to be made
     *                           (static, virtual, interface, or constructor)
     * @param delegateMethodName The name of the method to be called in the Painless script class
     * @param delegateMethodType The type of method call in the Painless script class without
     *                           the captured types
     * @return A {@link CallSite} linked to a factory method for creating a lambda class
     * that implements the expected functional interface
     * @throws LambdaConversionException Thrown when an illegal type conversion occurs at link time
     */
    public static CallSite lambdaBootstrap(
            Lookup lookup,
            String interfaceMethodName,
            MethodType factoryMethodType,
            MethodType interfaceMethodType,
            String delegateClassName,
            int delegateInvokeType,
            String delegateMethodName,
            MethodType delegateMethodType)
            throws LambdaConversionException {
        String factoryMethodName = "get$lambda";
        String lambdaClassName = lookup.lookupClass().getName().replace('.', '/') +
            "$$Lambda" + COUNTER.getAndIncrement();
        Type lambdaClassType = Type.getType("L" + lambdaClassName + ";");

        validateTypes(interfaceMethodType, delegateMethodType);

        ClassWriter cw =
            beginLambdaClass(lambdaClassName, factoryMethodType.returnType().getName());
        Capture[] captures = generateCaptureFields(cw, factoryMethodType);
        Method constructorMethod =
            generateLambdaConstructor(cw, lambdaClassType, factoryMethodType, captures);

        if (captures.length > 0) {
            generateFactoryMethod(
                cw, factoryMethodName, factoryMethodType, lambdaClassType, constructorMethod);
        }

        generateInterfaceMethod(cw, factoryMethodType, lambdaClassName, lambdaClassType,
            interfaceMethodName, interfaceMethodType, delegateClassName, delegateInvokeType,
            delegateMethodName, delegateMethodType, captures);
        endLambdaClass(cw);

        Class<?> lambdaClass =
            createLambdaClass((Loader)lookup.lookupClass().getClassLoader(), cw, lambdaClassName);

        if (captures.length > 0) {
            return createCaptureCallSite(lookup, factoryMethodName, factoryMethodType, lambdaClass);
        } else {
            return createNoCaptureCallSite(factoryMethodType, lambdaClass);
        }
    }

    /**
     * Validates some conversions at link time.  Currently, only ensures that the lambda method
     * with a return value cannot delegate to a delegate method with no return type.
     */
    private static void validateTypes(MethodType interfaceMethodType, MethodType delegateMethodType)
            throws LambdaConversionException {

        if (interfaceMethodType.returnType() != void.class &&
            delegateMethodType.returnType() == void.class) {
            throw new LambdaConversionException("lambda expects return type ["
                + interfaceMethodType.returnType() + "], but found return type [void]");
        }
    }

    /**
     * Creates the {@link ClassWriter} to be used for the lambda class generation.
     */
    private static ClassWriter beginLambdaClass(String lambdaClassName, String lambdaInterface) {
        String baseClass = Object.class.getName().replace('.', '/');
        lambdaInterface = lambdaInterface.replace('.', '/');
        int modifiers = ACC_PUBLIC | ACC_STATIC | ACC_SUPER | ACC_FINAL | ACC_SYNTHETIC;

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(CLASS_VERSION,
            modifiers, lambdaClassName, null, baseClass, new String[] {lambdaInterface});

        return cw;
    }

    /**
     * Generates member fields for captured variables
     * based on the parameters for the factory method.
     * @return An array of captured variable metadata
     * for generating method arguments later on
     */
    private static Capture[] generateCaptureFields(ClassWriter cw, MethodType factoryMethodType) {
        int captureTotal = factoryMethodType.parameterCount();
        Capture[] captures = new Capture[captureTotal];

        for (int captureCount = 0; captureCount < captureTotal; ++captureCount) {
            captures[captureCount] =
                new Capture(captureCount, factoryMethodType.parameterType(captureCount));
            int modifiers = ACC_PRIVATE + ACC_FINAL;

            FieldVisitor fv = cw.visitField(
                modifiers, captures[captureCount].name, captures[captureCount].desc, null, null);
            fv.visitEnd();
        }

        return captures;
    }

    /**
     * Generates a constructor that will take in captured
     * arguments if any and store them in their respective
     * member fields.
     * @return The constructor {@link Method} used to
     * call this method from a potential factory method
     * if there are captured arguments
     */
    private static Method generateLambdaConstructor(
            ClassWriter cw,
            Type lambdaClassType,
            MethodType factoryMethodType,
            Capture[] captures) {

        String conName = "<init>";
        String conDesc = factoryMethodType.changeReturnType(void.class).toMethodDescriptorString();
        Method conMeth = new Method(conName, conDesc);
        Type baseConType = Type.getType(Object.class);
        Method baseConMeth = new Method(conName,
            MethodType.methodType(void.class).toMethodDescriptorString());
        int modifiers = ACC_PUBLIC;

        GeneratorAdapter constructor = new GeneratorAdapter(modifiers, conMeth,
            cw.visitMethod(modifiers, conName, conDesc, null, null));
        constructor.visitCode();
        constructor.loadThis();
        constructor.invokeConstructor(baseConType, baseConMeth);

        for (int captureCount = 0; captureCount < captures.length; ++captureCount) {
            constructor.loadThis();
            constructor.loadArg(captureCount);
            constructor.putField(
                lambdaClassType, captures[captureCount].name, captures[captureCount].type);
        }

        constructor.returnValue();
        constructor.endMethod();

        return conMeth;
    }

    /**
     * Generates a factory method that can be used to create the lambda class
     * if there are captured variables.
     */
    private static void generateFactoryMethod(
            ClassWriter cw,
            String factoryMethodName,
            MethodType factoryMethodType,
            Type lambdaClassType,
            Method constructorMethod) {

        String facDesc = factoryMethodType.toMethodDescriptorString();
        Method facMeth = new Method(factoryMethodName, facDesc);
        int modifiers = ACC_PUBLIC | ACC_STATIC;

        GeneratorAdapter factory = new GeneratorAdapter(modifiers, facMeth,
            cw.visitMethod(modifiers, factoryMethodName, facDesc, null, null));
        factory.visitCode();
        factory.newInstance(lambdaClassType);
        factory.dup();
        factory.loadArgs();
        factory.invokeConstructor(lambdaClassType, constructorMethod);
        factory.returnValue();
        factory.endMethod();
    }

    /**
     * Generates the interface method that will delegate (call) to the delegate method.
     */
    private static void generateInterfaceMethod(
            ClassWriter cw,
            MethodType factoryMethodType,
            String lambdaClassName,
            Type lambdaClassType,
            String interfaceMethodName,
            MethodType interfaceMethodType,
            String delegateClassName,
            int delegateInvokeType,
            String delegateMethodName,
            MethodType delegateMethodType,
            Capture[] captures)
            throws LambdaConversionException {

        String lamDesc = interfaceMethodType.toMethodDescriptorString();
        Method lamMeth = new Method(lambdaClassName, lamDesc);
        int modifiers = ACC_PUBLIC;

        GeneratorAdapter iface = new GeneratorAdapter(modifiers, lamMeth,
            cw.visitMethod(modifiers, interfaceMethodName, lamDesc, null, null));
        iface.visitCode();

        // Handles the case where a reference method refers to a constructor.
        // A new instance of the requested type will be created and the
        // constructor with no parameters will be called.
        // Example: String::new
        if (delegateInvokeType == H_NEWINVOKESPECIAL) {
            String conName = "<init>";
            String conDesc = MethodType.methodType(void.class).toMethodDescriptorString();
            Method conMeth = new Method(conName, conDesc);
            Type conType = Type.getType(delegateMethodType.returnType());

            iface.newInstance(conType);
            iface.dup();
            iface.invokeConstructor(conType, conMeth);
        } else {
            // Loads any captured variables onto the stack.
            for (int captureCount = 0; captureCount < captures.length; ++captureCount) {
                iface.loadThis();
                iface.getField(
                    lambdaClassType, captures[captureCount].name, captures[captureCount].type);
            }

            // Loads any passed in arguments onto the stack.
            iface.loadArgs();

            // Handles the case for a lambda function or a static reference method.
            // interfaceMethodType and delegateMethodType both have the captured types
            // inserted into their type signatures.  This later allows the delegate
            // method to be invoked dynamically and have the interface method types
            // appropriately converted to the delegate method types.
            // Example: Integer::parseInt
            // Example: something.each(x -> x + 1)
            if (delegateInvokeType == H_INVOKESTATIC) {
                interfaceMethodType =
                    interfaceMethodType.insertParameterTypes(0, factoryMethodType.parameterArray());
                delegateMethodType =
                    delegateMethodType.insertParameterTypes(0, factoryMethodType.parameterArray());
            } else if (delegateInvokeType == H_INVOKEVIRTUAL ||
                delegateInvokeType == H_INVOKEINTERFACE) {
                // Handles the case for a virtual or interface reference method with no captures.
                // delegateMethodType drops the 'this' parameter because it will be re-inserted
                // when the method handle for the dynamically invoked delegate method is created.
                // Example: Object::toString
                if (captures.length == 0) {
                    Class<?> clazz = delegateMethodType.parameterType(0);
                    delegateClassName = clazz.getName();
                    delegateMethodType = delegateMethodType.dropParameterTypes(0, 1);
                // Handles the case for a virtual or interface reference method with 'this'
                // captured. interfaceMethodType inserts the 'this' type into its
                // method signature. This later allows the delegate
                // method to be invoked dynamically and have the interface method types
                // appropriately converted to the delegate method types.
                // Example: something::toString
                } else if (captures.length == 1) {
                    Class<?> clazz = factoryMethodType.parameterType(0);
                    delegateClassName = clazz.getName();
                    interfaceMethodType = interfaceMethodType.insertParameterTypes(0, clazz);
                } else {
                    throw new LambdaConversionException(
                        "unexpected number of captures [ " + captures.length + "]");
                }
            } else {
                throw new IllegalStateException(
                    "unexpected invocation type [" + delegateInvokeType + "]");
            }

            Handle delegateHandle =
                new Handle(delegateInvokeType, delegateClassName.replace('.', '/'),
                    delegateMethodName, delegateMethodType.toMethodDescriptorString(),
                    delegateInvokeType == H_INVOKEINTERFACE);
            iface.invokeDynamic(delegateMethodName, Type.getMethodType(interfaceMethodType
                    .toMethodDescriptorString()).getDescriptor(), DELEGATE_BOOTSTRAP_HANDLE,
                delegateHandle);
        }

        iface.returnValue();
        iface.endMethod();
    }

    /**
     * Closes the {@link ClassWriter}.
     */
    private static void endLambdaClass(ClassWriter cw) {
        cw.visitEnd();
    }

    /**
     * Defines the {@link Class} for the lambda class using the same {@link Loader}
     * that originally defined the class for the Painless script.
     */
    private static Class<?> createLambdaClass(
            Loader loader,
            ClassWriter cw,
            String lambdaClassName) {

        byte[] classBytes = cw.toByteArray();
        return AccessController.doPrivileged((PrivilegedAction<Class<?>>)() ->
            loader.defineLambda(lambdaClassName.replace('/', '.'), classBytes));
    }

    /**
     * Creates an {@link ConstantCallSite} that will return the same instance
     * of the generated lambda class every time this linked factory method is called.
     */
    private static CallSite createNoCaptureCallSite(
            MethodType factoryMethodType,
            Class<?> lambdaClass) {

        Constructor<?> constructor = AccessController.doPrivileged(
            (PrivilegedAction<Constructor<?>>)() -> {
                try {
                    return lambdaClass.getConstructor();
                } catch (NoSuchMethodException nsme) {
                    throw new IllegalStateException("unable to create lambda class", nsme);
                }
            });

        try {
            return new ConstantCallSite(MethodHandles.constant(
                factoryMethodType.returnType(), constructor.newInstance()));
        } catch (InstantiationException |
            IllegalAccessException |
            InvocationTargetException exception) {
            throw new IllegalStateException("unable to create lambda class", exception);
        }
    }

    /**
     * Creates an {@link ConstantCallSite}
     */
    private static CallSite createCaptureCallSite(
            Lookup lookup,
            String factoryMethodName,
            MethodType factoryMethodType,
            Class<?> lambdaClass) {

        try {
            return new ConstantCallSite(
                lookup.findStatic(lambdaClass, factoryMethodName, factoryMethodType));
        } catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new IllegalStateException("unable to create lambda factory class", exception);
        }
    }

    /**
     * Links the delegate method to the returned {@link CallSite}.  The linked
     * delegate method will use converted types from the interface method.  Using
     * invokedynamic to make the delegate method call allows
     * {@link MethodHandle#asType} to be used to do the type conversion instead
     * of either a lot more code or requiring many {@link Definition.Type}s to be looked
     * up at link-time.
     */
    public static CallSite delegateBootstrap(Lookup lookup,
                                             String delegateMethodName,
                                             MethodType interfaceMethodType,
                                             MethodHandle delegateMethodHandle) {
        return new ConstantCallSite(delegateMethodHandle.asType(interfaceMethodType));
    }
}
