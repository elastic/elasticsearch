/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.MethodModel;
import org.elasticsearch.foreign.processor.model.NativeType;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutArray;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.primitiveClassDesc;

/**
 * Generates {@code <InterfaceName>$Impl} class files for {@code @LibrarySpecification}-annotated interfaces.
 *
 * <p>Each generated class:
 * <ul>
 *   <li>is package-private {@code final} with a package-private no-arg constructor (only {@code $Provider} in the same package can instantiate it)</li>
 *   <li>implements the annotated interface</li>
 *   <li>has one {@code private static final MethodHandle} field per {@code @Function} method</li>
 *   <li>initializes those fields in {@code <clinit>}</li>
 *   <li>implements each interface method by calling {@code MethodHandle.invokeExact}</li>
 * </ul>
 */
class ImplClassWriter {

    private static final ClassDesc CD_MethodHandle = ClassDesc.of("java.lang.invoke.MethodHandle");
    private static final ClassDesc CD_FunctionDescriptor = ClassDesc.of("java.lang.foreign.FunctionDescriptor");
    private static final ClassDesc CD_LinkerOption = ClassDesc.of("java.lang.foreign.Linker$Option");
    private static final ClassDesc CD_LinkerOptionArray = ClassDesc.ofDescriptor("[Ljava/lang/foreign/Linker$Option;");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Throwable = ClassDesc.of("java.lang.Throwable");
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of("org.elasticsearch.foreign.LinkerHelper");
    private static final ClassDesc CD_LinkerHelperUtil = ClassDesc.of("org.elasticsearch.foreign.LinkerHelperUtil");
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of("org.elasticsearch.foreign.LoaderHelper");
    private final Filer filer;
    private final int classFileVersion;

    ImplClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /**
     * Generates and writes the {@code $Impl} class for the given library model.
     */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        String generatedName = model.packageName().isEmpty()
            ? model.simpleName() + "$Impl"
            : model.packageName() + "." + model.simpleName() + "$Impl";
        ClassDesc generatedDesc = ClassDesc.of(generatedName);
        ClassDesc interfaceDesc = ClassDesc.of(model.qualifiedName());

        List<MethodModel> nativeMethods = model.methods();

        byte[] classBytes = ClassFile.of().build(generatedDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(interfaceDesc);

            // MethodHandle fields: one per @Function method
            for (var nm : nativeMethods) {
                cb.withField(
                    nm.methodName() + "$mh",
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // <clinit>: initialize all static fields
            cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
                if (model.libraryName().isEmpty() == false) {
                    emitLoadLibrary(clinit, model.libraryName());
                }
                for (var nm : nativeMethods) {
                    emitMhFieldInit(clinit, generatedDesc, nm);
                }
                clinit.return_();
            });

            // <init>: package-private no-arg constructor (only $Provider in the same package calls this)
            cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
                init.aload(0);
                init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
                init.return_();
            });

            // @Function method implementations
            for (var nm : nativeMethods) {
                emitNativeFunctionMethod(cb, generatedDesc, nm);
            }
        });

        try (var os = filer.createClassFile(generatedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    // -------------------------------------------------------------------------
    // <clinit> helpers
    // -------------------------------------------------------------------------

    private static void emitLoadLibrary(java.lang.classfile.CodeBuilder cb, String libName) {
        cb.ldc(libName);
        cb.invokestatic(CD_LoaderHelper, "loadLibrary", MethodTypeDesc.of(CD_void, CD_String));
    }

    /**
     * Resolves the native symbol and links it to a MethodHandle stored in {@code <name>$mh}.
     * This handle is what the generated method implementation calls at runtime.
     */
    private static void emitMhFieldInit(java.lang.classfile.CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        String fieldName = nm.methodName() + "$mh";

        cb.ldc(nm.cSymbol());

        emitFunctionDescriptor(cb, nm.returnType(), nativeParamTypes(nm));
        emitLinkerOptions(cb, nm);
        emitDowncallHandleCall(cb, nm);
        cb.putstatic(generatedDesc, fieldName, CD_MethodHandle);
    }

    private static void emitFunctionDescriptor(
        java.lang.classfile.CodeBuilder cb,
        MethodModel.ReturnType returnType,
        List<NativeType> paramTypes
    ) {
        int paramCount = paramTypes.size();

        if (returnType.type() == NativeType.VOID) {
            cb.loadConstant(paramCount);
            cb.anewarray(CD_MemoryLayout);
            for (int i = 0; i < paramCount; i++) {
                cb.dup();
                cb.loadConstant(i);
                emitValueLayout(cb, paramTypes.get(i));
                cb.aastore();
            }
            // FunctionDescriptor is an interface; isInterface=true
            cb.invokestatic(CD_FunctionDescriptor, "ofVoid", MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayoutArray), true);
        } else {
            emitValueLayout(cb, nativeLayoutType(returnType.type()));
            cb.loadConstant(paramCount);
            cb.anewarray(CD_MemoryLayout);
            for (int i = 0; i < paramCount; i++) {
                cb.dup();
                cb.loadConstant(i);
                emitValueLayout(cb, paramTypes.get(i));
                cb.aastore();
            }
            // FunctionDescriptor is an interface; isInterface=true
            cb.invokestatic(
                CD_FunctionDescriptor,
                "of",
                MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayout, CD_MemoryLayoutArray),
                true
            );
        }
    }

    private static void emitLinkerOptions(java.lang.classfile.CodeBuilder cb, MethodModel nm) {
        if (nm.isCritical()) {
            cb.invokestatic(CD_LinkerHelperUtil, "critical", MethodTypeDesc.of(CD_LinkerOptionArray));
        } else {
            cb.iconst_0();
            cb.anewarray(CD_LinkerOption);
        }
    }

    private static void emitDowncallHandleCall(java.lang.classfile.CodeBuilder cb, MethodModel nm) {
        MethodTypeDesc downcallDesc = MethodTypeDesc.of(CD_MethodHandle, CD_String, CD_FunctionDescriptor, CD_LinkerOptionArray);
        cb.invokestatic(CD_LinkerHelper, "downcallHandle", downcallDesc);
    }

    // -------------------------------------------------------------------------
    // Method body generation
    // -------------------------------------------------------------------------

    private static void emitNativeFunctionMethod(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        MethodTypeDesc javaDesc = buildJavaMethodDesc(nm);
        cb.withMethodBody(nm.methodName(), javaDesc, ClassFile.ACC_PUBLIC, code -> {
            code.trying(tryBlock -> {
                emitInvokeExact(tryBlock, generatedDesc, nm);
                emitTypedReturn(tryBlock, nm.returnType());
            }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                // throw new AssertionError(t)
                // Stack: [t]
                catchBlock.new_(CD_AssertionError);
                catchBlock.dup_x1();
                catchBlock.swap();
                catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
                catchBlock.athrow();
            }));
        });
    }

    /**
     * Invokes the native function through its downcall MethodHandle, marshaling each parameter to its
     * FFM-compatible type.
     */
    private static void emitInvokeExact(java.lang.classfile.CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        // Load the MethodHandle
        cb.getstatic(generatedDesc, nm.methodName() + "$mh", CD_MethodHandle);

        // Load each parameter
        int slot = 1; // slot 0 = this
        for (var param : nm.paramTypes()) {
            slot += emitLoadParam(cb, param, slot);
        }

        // Call invokeExact with the exact native descriptor
        MethodTypeDesc invokeExactDesc = buildInvokeExactDesc(nm);
        cb.invokevirtual(CD_MethodHandle, "invokeExact", invokeExactDesc);
    }

    /**
     * Marshals a single parameter to its FFM-compatible form for the native call.
     * Returns the number of local variable slots consumed (2 for {@code long}/{@code double}, 1 for others).
     */
    private static int emitLoadParam(java.lang.classfile.CodeBuilder cb, MethodModel.ParamInfo param, int slot) {
        switch (param.type()) {
            case INT, SHORT, BYTE, BOOLEAN -> {
                cb.iload(slot);
                return 1;
            }
            case LONG -> {
                cb.lload(slot);
                return 2;
            }
            case FLOAT -> {
                cb.fload(slot);
                return 1;
            }
            case DOUBLE -> {
                cb.dload(slot);
                return 2;
            }
            case ADDRESS -> {
                cb.aload(slot);
                return 1;
            }
            default -> throw new AssertionError("Unhandled param type: " + param.type());
        }
    }

    private static void emitTypedReturn(java.lang.classfile.CodeBuilder cb, MethodModel.ReturnType returnType) {
        if (returnType.type() == NativeType.VOID) {
            cb.return_();
            return;
        }
        switch (returnType.type()) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case STRING -> {
                // invokeExact returned a MemorySegment. If its address is 0 (null pointer), return null.
                // Otherwise call .reinterpret(Long.MAX_VALUE).getString(0).
                // Stack: [segment]
                var notNull = cb.newLabel();
                cb.dup();
                cb.invokeinterface(CD_MemorySegment, "address", MethodTypeDesc.of(CD_long));
                cb.lconst_0();
                cb.lcmp();
                cb.ifne(notNull);
                // null pointer path: pop segment, return null
                cb.pop();
                cb.aconst_null();
                cb.areturn();
                cb.labelBinding(notNull);
                cb.ldc(Long.MAX_VALUE);
                cb.invokeinterface(CD_MemorySegment, "reinterpret", MethodTypeDesc.of(CD_MemorySegment, CD_long));
                cb.ldc(0L);
                cb.invokeinterface(CD_MemorySegment, "getString", MethodTypeDesc.of(CD_String, CD_long));
                cb.areturn();
            }
            case VOID -> throw new AssertionError("void handled above");
        }
    }

    // -------------------------------------------------------------------------
    // Descriptor helpers
    // -------------------------------------------------------------------------

    /** Builds the Java-facing method descriptor, using Java types for all parameters and the return type. */
    private static MethodTypeDesc buildJavaMethodDesc(MethodModel nm) {
        ClassDesc returnDesc = javaClassDesc(nm.returnType());
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var p : nm.paramTypes()) {
            paramDescs.add(javaParamClassDesc(p));
        }
        return MethodTypeDesc.of(returnDesc, paramDescs);
    }

    /**
     * Builds the native-side descriptor used when calling through the downcall MethodHandle. String
     * returns become MemorySegment (marshaling happens after the call).
     */
    private static MethodTypeDesc buildInvokeExactDesc(MethodModel nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var p : nm.paramTypes()) {
            paramDescs.add(nativeClassDesc(p.type()));
        }
        ClassDesc returnDesc;
        if (nm.returnType().type() == NativeType.VOID) {
            returnDesc = CD_void;
        } else if (nm.returnType().type() == NativeType.STRING) {
            returnDesc = CD_MemorySegment;
        } else {
            returnDesc = nativeClassDesc(nm.returnType().type());
        }
        return MethodTypeDesc.of(returnDesc, paramDescs);
    }

    private static ClassDesc javaClassDesc(MethodModel.ReturnType returnType) {
        if (returnType.type() == NativeType.VOID) {
            return CD_void;
        }
        return switch (returnType.type()) {
            case ADDRESS -> CD_MemorySegment;
            case STRING -> CD_String;
            case VOID -> throw new AssertionError("void handled above");
            default -> primitiveClassDesc(returnType.type());
        };
    }

    private static ClassDesc javaParamClassDesc(MethodModel.ParamInfo param) {
        return switch (param.type()) {
            case ADDRESS -> CD_MemorySegment;
            case VOID -> throw new AssertionError("void cannot be a parameter type");
            default -> primitiveClassDesc(param.type());
        };
    }

    private static ClassDesc nativeClassDesc(NativeType type) {
        return switch (type) {
            case ADDRESS, STRING -> CD_MemorySegment;
            case VOID -> throw new AssertionError("void is not a native type");
            default -> primitiveClassDesc(type);
        };
    }

    /** STRING and ADDRESS both occupy an address-sized slot in the native layout. */
    private static NativeType nativeLayoutType(NativeType type) {
        return switch (type) {
            case STRING -> NativeType.ADDRESS;
            case VOID -> throw new AssertionError("void has no native layout");
            default -> type;
        };
    }

    /**
     * Returns the parameter types to include in the native FunctionDescriptor.
     */
    private static List<NativeType> nativeParamTypes(MethodModel nm) {
        List<NativeType> result = new ArrayList<>();
        for (var param : nm.paramTypes()) {
            result.add(param.type());
        }
        return result;
    }
}
