/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.invoke.MethodHandles.Lookup;
import static org.elasticsearch.painless.WriterConstants.CLASS_VERSION;
import static org.elasticsearch.painless.WriterConstants.CTOR_METHOD_NAME;
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
 * <p>For each lambda function/method reference used within a Painless script
 * a class will be generated at link-time using the
 * {@link LambdaBootstrap#lambdaBootstrap} method that contains the following:
 *
 * <ol>
 * <li>member fields for any captured variables
 * <li>a constructor that will take in captured variables and assign them to
 * their respective member fields
 * <li>a static ctor delegation method, if the lambda function is a ctor.
 * <li>a method that will load the member fields representing captured variables
 * and take in any other necessary values based on the arguments passed into the
 * lambda function/reference method; it will then make a delegated call to the
 * actual lambda function/reference method.
 * </ol>
 *
 * <p>Take for example the following Painless script:
 *
 * <pre>{@code
 * List list1 = new ArrayList(); "
 * list1.add(2); "
 * List list2 = new ArrayList(); "
 * list1.forEach(x -> list2.add(x));"
 * return list[0]"
 * }</pre>
 *
 * <p>The script contains a lambda function with a captured variable.
 * The following Lambda class would be generated:
 *
 * <pre>{@code
 *     public static final class $$Lambda0 implements Consumer {
 *         private List arg$0;
 *
 *         private $$Lambda0(List arg$0) {
 *             this.arg$0 = arg$0;
 *         }
 *
 *         public static Consumer create$lambda(List arg$0) {
 *             return new $$Lambda0(arg$0);
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
 * }</pre>
 *
 * <p>Also the accept method actually uses an invokedynamic
 * instruction to call the lambda$0 method so that
 * {@link MethodHandle#asType} can be used to do the necessary
 * conversions between argument types without having to hard
 * code them. For method references to a constructor, a static
 * wrapper method is created, that creates a class instance and
 * calls the constructor. This method is used by the
 * invokedynamic call to initialize the instance.
 *
 * <p>When the {@link CallSite} is linked the linked method depends
 * on whether or not there are captures.  If there are no captures
 * the same instance of the generated lambda class will be
 * returned each time by the factory method as there are no
 * changing values other than the arguments, the lambda is a singleton.
 * If there are captures, a new instance of the generated lambda class
 * will be returned each time with the captures passed into the
 * factory method to be stored in the member fields.
 * Instead of calling the ctor, a static factory method is created
 * in the lambda class, because a method handle to the ctor directly
 * is (currently) preventing Hotspot optimizer from correctly doing
 * escape analysis. Escape analysis is important to optimize the
 * code in a way, that a new instance is not created on each lambda
 * invocation with captures, stressing garbage collector (thanks
 * to Rémi Forax for the explanation about this on Jaxcon 2017!).
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
     * This method name is used to generate a static wrapper method to handle delegation of ctors.
     */
    private static final String DELEGATED_CTOR_WRAPPER_NAME = "delegate$ctor";

    /**
     * This method name is used to generate the static factory for capturing lambdas.
     */
    private static final String LAMBDA_FACTORY_METHOD_NAME = "create$lambda";

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
     * @param delegateClassName The name of the class to delegate method call to
     * @param delegateInvokeType The type of method call to be made
     *                           (static, virtual, interface, or constructor)
     * @param delegateMethodName The name of the method to be called in the Painless script class
     * @param delegateMethodType The type of method call in the Painless script class without
     *                           the captured types
     * @param isDelegateInterface If the method to be called is owned by an interface where
     *                            if the value is '1' if the delegate is an interface and '0'
     *                            otherwise; note this is an int because the bootstrap method
     *                            cannot convert constants to boolean
     * @param injections Optionally add injectable constants into a method reference
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
            MethodType delegateMethodType,
            int isDelegateInterface,
            int isDelegateAugmented,
            Object... injections)
            throws LambdaConversionException {
        Compiler.Loader loader = (Compiler.Loader)lookup.lookupClass().getClassLoader();
        String lambdaClassName = Type.getInternalName(lookup.lookupClass()) + "$$Lambda" + loader.newLambdaIdentifier();
        Type lambdaClassType = Type.getObjectType(lambdaClassName);
        Type delegateClassType = Type.getObjectType(delegateClassName.replace('.', '/'));

        validateTypes(interfaceMethodType, delegateMethodType);

        ClassWriter cw = beginLambdaClass(lambdaClassName, factoryMethodType.returnType());
        Capture[] captures = generateCaptureFields(cw, factoryMethodType);
        generateLambdaConstructor(cw, lambdaClassType, factoryMethodType, captures);

        // Handles the special case where a method reference refers to a ctor (we need a static wrapper method):
        if (delegateInvokeType == H_NEWINVOKESPECIAL) {
            assert CTOR_METHOD_NAME.equals(delegateMethodName);
            generateStaticCtorDelegator(cw, ACC_PRIVATE, DELEGATED_CTOR_WRAPPER_NAME, delegateClassType, delegateMethodType);
            // replace the delegate with our static wrapper:
            delegateMethodName = DELEGATED_CTOR_WRAPPER_NAME;
            delegateClassType = lambdaClassType;
            delegateInvokeType = H_INVOKESTATIC;
        }

        generateInterfaceMethod(cw, factoryMethodType, lambdaClassType, interfaceMethodName,
            interfaceMethodType, delegateClassType, delegateInvokeType,
            delegateMethodName, delegateMethodType, isDelegateInterface == 1, isDelegateAugmented == 1, captures, injections);

        endLambdaClass(cw);

        Class<?> lambdaClass = createLambdaClass(loader, cw, lambdaClassType);
        if (captures.length > 0) {
            return createCaptureCallSite(lookup, factoryMethodType, lambdaClass);
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
    private static ClassWriter beginLambdaClass(String lambdaClassName, Class<?> lambdaInterface) {
        String baseClass = Type.getInternalName(Object.class);
        int modifiers = ACC_PUBLIC | ACC_SUPER | ACC_FINAL | ACC_SYNTHETIC;

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(CLASS_VERSION,
            modifiers, lambdaClassName, null, baseClass, new String[] { Type.getInternalName(lambdaInterface) });

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
            int modifiers = ACC_PRIVATE | ACC_FINAL;

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
     */
    private static void generateLambdaConstructor(
            ClassWriter cw,
            Type lambdaClassType,
            MethodType factoryMethodType,
            Capture[] captures) {

        String conDesc = factoryMethodType.changeReturnType(void.class).toMethodDescriptorString();
        Method conMeth = new Method(CTOR_METHOD_NAME, conDesc);
        Type baseConType = Type.getType(Object.class);
        Method baseConMeth = new Method(CTOR_METHOD_NAME,
            MethodType.methodType(void.class).toMethodDescriptorString());
        int modifiers = (captures.length > 0) ? ACC_PRIVATE : ACC_PUBLIC;

        GeneratorAdapter constructor = new GeneratorAdapter(modifiers, conMeth,
            cw.visitMethod(modifiers, CTOR_METHOD_NAME, conDesc, null, null));
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

        // Add a factory method, if lambda takes captures.
        // @uschindler says: I talked with Rémi Forax about this. Technically, a plain ctor
        // and a MethodHandle to the ctor would be enough - BUT: Hotspot is unable to
        // do escape analysis through a MethodHandles.findConstructor generated handle.
        // Because of this we create a factory method. With this factory method, the
        // escape analysis can figure out that everything is final and we don't need
        // an instance, so it can omit object creation on heap!
        if (captures.length > 0) {
            generateStaticCtorDelegator(cw, ACC_PUBLIC, LAMBDA_FACTORY_METHOD_NAME, lambdaClassType, factoryMethodType);
        }
    }

    /**
     * Generates a factory method to delegate to constructors.
     */
    private static void generateStaticCtorDelegator(ClassWriter cw, int access, String delegatorMethodName,
            Type delegateClassType, MethodType delegateMethodType) {
        Method wrapperMethod = new Method(delegatorMethodName, delegateMethodType.toMethodDescriptorString());
        Method constructorMethod =
            new Method(CTOR_METHOD_NAME, delegateMethodType.changeReturnType(void.class).toMethodDescriptorString());
        int modifiers = access | ACC_STATIC;

        GeneratorAdapter factory = new GeneratorAdapter(modifiers, wrapperMethod,
            cw.visitMethod(modifiers, delegatorMethodName, delegateMethodType.toMethodDescriptorString(), null, null));
        factory.visitCode();
        factory.newInstance(delegateClassType);
        factory.dup();
        factory.loadArgs();
        factory.invokeConstructor(delegateClassType, constructorMethod);
        factory.returnValue();
        factory.endMethod();
    }

    /**
     * Generates the interface method that will delegate (call) to the delegate method
     * with {@code INVOKEDYNAMIC} using the {@link #delegateBootstrap} type converter.
     */
    private static void generateInterfaceMethod(
            ClassWriter cw,
            MethodType factoryMethodType,
            Type lambdaClassType,
            String interfaceMethodName,
            MethodType interfaceMethodType,
            Type delegateClassType,
            int delegateInvokeType,
            String delegateMethodName,
            MethodType delegateMethodType,
            boolean isDelegateInterface,
            boolean isDelegateAugmented,
            Capture[] captures,
            Object... injections)
            throws LambdaConversionException {

        String lamDesc = interfaceMethodType.toMethodDescriptorString();
        Method lamMeth = new Method(lambdaClassType.getInternalName(), lamDesc);
        int modifiers = ACC_PUBLIC;

        GeneratorAdapter iface = new GeneratorAdapter(modifiers, lamMeth,
            cw.visitMethod(modifiers, interfaceMethodName, lamDesc, null, null));
        iface.visitCode();

        // Loads any captured variables onto the stack.
        for (int captureCount = 0; captureCount < captures.length; ++captureCount) {
            iface.loadThis();
            iface.getField(
                lambdaClassType, captures[captureCount].name, captures[captureCount].type);
        }

        // Loads any passed in arguments onto the stack.
        iface.loadArgs();

        String functionalInterfaceWithCaptures;

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
            functionalInterfaceWithCaptures = interfaceMethodType.toMethodDescriptorString();
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
                delegateClassType = Type.getType(clazz);
                delegateMethodType = delegateMethodType.dropParameterTypes(0, 1);
                functionalInterfaceWithCaptures = interfaceMethodType.toMethodDescriptorString();
            // Handles the case for a virtual or interface reference method with 'this'
            // captured. interfaceMethodType inserts the 'this' type into its
            // method signature. This later allows the delegate
            // method to be invoked dynamically and have the interface method types
            // appropriately converted to the delegate method types.
            // Example: something::toString
            } else {
                Class<?> clazz = factoryMethodType.parameterType(0);
                delegateClassType = Type.getType(clazz);

                // functionalInterfaceWithCaptures needs to add the receiver and other captures
                List<Type> parameters = interfaceMethodType.parameterList().stream().map(Type::getType).collect(Collectors.toList());
                parameters.add(0,  delegateClassType);
                for (int i = 1; i < captures.length; i++) {
                    parameters.add(i, captures[i].type);
                }
                Type[] parametersArray = parameters.toArray(new Type[0]);
                functionalInterfaceWithCaptures = Type.getMethodDescriptor(Type.getType(interfaceMethodType.returnType()), parametersArray);

                // delegateMethod does not need the receiver
                List<Class<?>> factoryParameters = factoryMethodType.parameterList();
                if (factoryParameters.size() > 1) {
                    List<Class<?>> factoryParametersWithReceiver = factoryParameters.subList(1, factoryParameters.size());
                    delegateMethodType = delegateMethodType.insertParameterTypes(0, factoryParametersWithReceiver);
                }
            }
        } else {
            throw new IllegalStateException(
                "unexpected invocation type [" + delegateInvokeType + "]");
        }

        Handle delegateHandle =
            new Handle(delegateInvokeType, delegateClassType.getInternalName(),
                delegateMethodName, delegateMethodType.toMethodDescriptorString(),
                isDelegateInterface);
        // Fill in args for indy. Always add the delegate handle and
        // whether it's static or not then injections as necessary.
        Object[] args = new Object[2 + injections.length];
        args[0] = delegateHandle;
        args[1] = delegateInvokeType == H_INVOKESTATIC && isDelegateAugmented == false ? 0 : 1;
        System.arraycopy(injections, 0, args, 2, injections.length);
        iface.invokeDynamic(
                delegateMethodName,
                Type.getMethodType(functionalInterfaceWithCaptures).getDescriptor(),
                DELEGATE_BOOTSTRAP_HANDLE,
                args);

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
     * Defines the {@link Class} for the lambda class using the same {@link Compiler.Loader}
     * that originally defined the class for the Painless script.
     */
    private static Class<?> createLambdaClass(
            Compiler.Loader loader,
            ClassWriter cw,
            Type lambdaClassType) {

        byte[] classBytes = cw.toByteArray();
        // DEBUG:
        // new ClassReader(classBytes).accept(new TraceClassVisitor(new PrintWriter(System.out)), ClassReader.SKIP_DEBUG);
        return AccessController.doPrivileged((PrivilegedAction<Class<?>>)() ->
            loader.defineLambda(lambdaClassType.getClassName(), classBytes));
    }

    /**
     * Creates an {@link ConstantCallSite} that will return the same instance
     * of the generated lambda class every time this linked factory method is called.
     */
    private static CallSite createNoCaptureCallSite(
            MethodType factoryMethodType,
            Class<?> lambdaClass) {

        try {
            return new ConstantCallSite(MethodHandles.constant(
                factoryMethodType.returnType(), lambdaClass.getConstructor().newInstance()));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("unable to instantiate lambda class", exception);
        }
    }

    /**
     * Creates an {@link ConstantCallSite}
     */
    private static CallSite createCaptureCallSite(
            Lookup lookup,
            MethodType factoryMethodType,
            Class<?> lambdaClass) {

        try {
            return new ConstantCallSite(
                lookup.findStatic(lambdaClass, LAMBDA_FACTORY_METHOD_NAME, factoryMethodType));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("unable to create lambda class", exception);
        }
    }

    /**
     * Links the delegate method to the returned {@link CallSite}.  The linked
     * delegate method will use converted types from the interface method.  Using
     * invokedynamic to make the delegate method call allows
     * {@link MethodHandle#asType} to be used to do the type conversion instead
     * of either a lot more code or requiring many {@link Class}es to be looked
     * up at link-time.
     */
    public static CallSite delegateBootstrap(Lookup lookup,
                                             String delegateMethodName,
                                             MethodType interfaceMethodType,
                                             MethodHandle delegateMethodHandle,
                                             int isVirtual,
                                             Object... injections) {

        if (injections.length > 0) {
            delegateMethodHandle = MethodHandles.insertArguments(delegateMethodHandle, isVirtual, injections);
        }

        return new ConstantCallSite(delegateMethodHandle.asType(interfaceMethodType));
    }
}
