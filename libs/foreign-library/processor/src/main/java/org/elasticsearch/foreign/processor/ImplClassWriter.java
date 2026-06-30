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

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
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
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegmentAdapter;
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
 *   <li>is package-private {@code final} with a package-private no-arg constructor (only {@code $Provider}
 *   in the same package can instantiate it)</li>
 *   <li>implements the annotated interface</li>
 *   <li>has one {@code private static final MethodHandle} field per {@code @Function} method</li>
 *   <li>initializes those fields in {@code <clinit>}</li>
 *   <li>implements each interface method by calling {@code MethodHandle.invokeExact}</li>
 * </ul>
 */
class ImplClassWriter {

    private static final ClassDesc CD_MethodHandle = ClassDesc.of("java.lang.invoke.MethodHandle");
    private static final ClassDesc CD_MethodHandles = ClassDesc.of("java.lang.invoke.MethodHandles");
    private static final ClassDesc CD_Lookup = ClassDesc.of("java.lang.invoke.MethodHandles$Lookup");
    private static final ClassDesc CD_FunctionDescriptor = ClassDesc.of("java.lang.foreign.FunctionDescriptor");
    private static final ClassDesc CD_LinkerOption = ClassDesc.of("java.lang.foreign.Linker$Option");
    private static final ClassDesc CD_LinkerOptionArray = ClassDesc.ofDescriptor("[Ljava/lang/foreign/Linker$Option;");
    private static final ClassDesc CD_AssertionError = ClassDesc.of("java.lang.AssertionError");
    private static final ClassDesc CD_Throwable = ClassDesc.of("java.lang.Throwable");
    private static final ClassDesc CD_Class = ClassDesc.of("java.lang.Class");
    private static final ClassDesc CD_Arena = ClassDesc.of("java.lang.foreign.Arena");
    private static final ClassDesc CD_LinkerHelper = ClassDesc.of("org.elasticsearch.foreign.LinkerHelper");
    private static final ClassDesc CD_LinkerAdapter = ClassDesc.of("org.elasticsearch.foreign.adapter.LinkerAdapter");
    private static final ClassDesc CD_LoaderHelper = ClassDesc.of("org.elasticsearch.foreign.LoaderHelper");

    private static final MethodTypeDesc MTD_FunctionDescriptor_ofVoid = MethodTypeDesc.of(CD_FunctionDescriptor, CD_MemoryLayoutArray);
    private static final MethodTypeDesc MTD_FunctionDescriptor_of = MethodTypeDesc.of(
        CD_FunctionDescriptor,
        CD_MemoryLayout,
        CD_MemoryLayoutArray
    );
    private static final MethodTypeDesc MTD_downcallHandle = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_String,
        CD_FunctionDescriptor,
        CD_LinkerOptionArray
    );
    private static final MethodTypeDesc MTD_adaptCritical = MethodTypeDesc.of(
        CD_MethodHandle,
        CD_Lookup,
        CD_MethodHandle,
        CD_Class,
        CD_String
    );
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_getString = MethodTypeDesc.of(CD_String, CD_MemorySegment, CD_long);
    private static final MethodTypeDesc MTD_Arena_ofConfined = MethodTypeDesc.of(CD_Arena);
    private static final MethodTypeDesc MTD_Arena_close = MethodTypeDesc.of(CD_void);
    private static final MethodTypeDesc MTD_MemorySegmentAdapter_allocateString = MethodTypeDesc.of(CD_MemorySegment, CD_Arena, CD_String);

    private final Filer filer;
    private final int classFileVersion;

    ImplClassWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /** Generates and writes the {@code $Impl} class for the given library model. */
    void generate(LibraryModel model, TypeElement sourceElement) throws Exception {
        ClassDesc generatedDesc = ClassDesc.of(model.implQualifiedName());
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
                    nm.methodHandleFieldName(),
                    CD_MethodHandle,
                    fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL)
                );
            }

            // <clinit>: load the library and initialize each MethodHandle field
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

        try (var os = filer.createClassFile(model.implQualifiedName(), sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    // -------------------------------------------------------------------------
    // <clinit> helpers
    // -------------------------------------------------------------------------

    private static void emitLoadLibrary(CodeBuilder cb, String libName) {
        cb.ldc(libName);
        cb.invokestatic(CD_LoaderHelper, "loadLibrary", MethodTypeDesc.of(CD_void, CD_String));
    }

    /**
     * Resolves the native symbol and stores the resulting {@code MethodHandle} in the static
     * {@code <name>$mh} field. This handle is what the generated method body calls at runtime.
     */
    private static void emitMhFieldInit(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        boolean hasFallbackAdapter = nm.fallbackAdapterClassName() != null;

        // For @Critical methods with a fallback adapter we need to call
        // LinkerAdapter.adaptCritical(lookup, rawHandle, adapterClass, methodName). Stack-prep
        // the leading lookup arg here, then build the raw handle on top.
        if (hasFallbackAdapter) {
            cb.invokestatic(CD_MethodHandles, "lookup", MethodTypeDesc.of(CD_Lookup));
        }

        cb.ldc(nm.cSymbol());
        emitFunctionDescriptor(cb, nm.returnType(), nm.paramTypes());
        emitLinkerOptions(cb, nm);
        cb.invokestatic(CD_LinkerHelper, "downcallHandle", MTD_downcallHandle);

        if (hasFallbackAdapter) {
            cb.ldc(ClassDesc.of(nm.fallbackAdapterClassName()));
            cb.ldc(nm.methodName());
            cb.invokestatic(CD_LinkerAdapter, "adaptCritical", MTD_adaptCritical);
        }

        cb.putstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);
    }

    private static void emitFunctionDescriptor(CodeBuilder cb, NativeType returnType, List<NativeType> paramTypes) {
        // FunctionDescriptor is an interface, so invokestatic uses isInterface=true.
        if (returnType == NativeType.VOID) {
            emitParamLayoutArray(cb, paramTypes);
            cb.invokestatic(CD_FunctionDescriptor, "ofVoid", MTD_FunctionDescriptor_ofVoid, true);
        } else {
            emitValueLayout(cb, returnType.layoutType());
            emitParamLayoutArray(cb, paramTypes);
            cb.invokestatic(CD_FunctionDescriptor, "of", MTD_FunctionDescriptor_of, true);
        }
    }

    private static void emitParamLayoutArray(CodeBuilder cb, List<NativeType> paramTypes) {
        cb.loadConstant(paramTypes.size());
        cb.anewarray(CD_MemoryLayout);
        for (int i = 0; i < paramTypes.size(); i++) {
            cb.dup();
            cb.loadConstant(i);
            emitValueLayout(cb, paramTypes.get(i).layoutType());
            cb.aastore();
        }
    }

    private static void emitLinkerOptions(CodeBuilder cb, MethodModel nm) {
        if (nm.isCritical()) {
            cb.invokestatic(CD_LinkerAdapter, "critical", MethodTypeDesc.of(CD_LinkerOptionArray));
        } else {
            cb.iconst_0();
            cb.anewarray(CD_LinkerOption);
        }
    }

    // -------------------------------------------------------------------------
    // Method body generation
    // -------------------------------------------------------------------------

    private static void emitNativeFunctionMethod(ClassBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        cb.withMethodBody(nm.methodName(), buildJavaMethodDesc(nm), ClassFile.ACC_PUBLIC, code -> {
            boolean hasStringParams = nm.paramTypes().contains(NativeType.STRING);
            if (hasStringParams) {
                emitNativeFunctionMethodWithStringParams(code, generatedDesc, nm);
            } else {
                code.trying(tryBlock -> {
                    emitInvokeExact(tryBlock, generatedDesc, nm);
                    emitTypedReturn(tryBlock, nm.returnType());
                }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
                    // throw new AssertionError(t) — stack on entry: [t]
                    catchBlock.new_(CD_AssertionError);
                    catchBlock.dup_x1();
                    catchBlock.swap();
                    catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
                    catchBlock.athrow();
                }));
            }
        });
    }

    /**
     * Generates a method body that marshals {@code String} parameters to native memory before the call.
     * Opens a confined {@code Arena} per call, allocates each {@code String} param via
     * {@code MemorySegmentUtil.allocateString}, and closes the arena in both normal and exception paths.
     *
     * <p>Local variable layout (slots):
     * <ul>
     *   <li>0: {@code this}</li>
     *   <li>1..paramEnd-1: original Java parameters</li>
     *   <li>paramEnd: the {@code Arena}</li>
     *   <li>paramEnd+1..: one {@code MemorySegment} per {@code STRING} parameter, in order</li>
     *   <li>last slot (if non-void return): the return value from invokeExact</li>
     * </ul>
     */
    private static void emitNativeFunctionMethodWithStringParams(CodeBuilder code, ClassDesc generatedDesc, MethodModel nm) {
        List<NativeType> paramTypes = nm.paramTypes();
        NativeType returnType = nm.returnType();

        // Compute where params end and arena+marshaled-string locals begin.
        int paramSlotsEnd = 1; // slot 0 = this
        for (NativeType t : paramTypes) {
            paramSlotsEnd += (t == NativeType.LONG || t == NativeType.DOUBLE) ? 2 : 1;
        }
        int arenaSlot = paramSlotsEnd;

        // Count STRING params to know how many marshaled slots we need.
        long stringParamCount = paramTypes.stream().filter(t -> t == NativeType.STRING).count();
        int resultSlot = arenaSlot + 1 + (int) stringParamCount;

        // Arena arena = Arena.ofConfined()
        code.invokestatic(CD_Arena, "ofConfined", MTD_Arena_ofConfined, true);
        code.astore(arenaSlot);

        code.trying(tryBlock -> {
            // Marshal each String param: MemorySegment $sN = MemorySegmentUtil.allocateString(arena, strN)
            int slot = 1;
            int marshaledSlot = arenaSlot + 1;
            for (NativeType paramType : paramTypes) {
                if (paramType == NativeType.STRING) {
                    tryBlock.aload(arenaSlot);
                    tryBlock.aload(slot);
                    tryBlock.invokestatic(CD_MemorySegmentAdapter, "allocateString", MTD_MemorySegmentAdapter_allocateString);
                    tryBlock.astore(marshaledSlot);
                    marshaledSlot++;
                }
                slot += (paramType == NativeType.LONG || paramType == NativeType.DOUBLE) ? 2 : 1;
            }

            // Push method handle, then all params (String params → their marshaled MemorySegment slots)
            tryBlock.getstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);
            slot = 1;
            marshaledSlot = arenaSlot + 1;
            for (NativeType paramType : paramTypes) {
                if (paramType == NativeType.STRING) {
                    tryBlock.aload(marshaledSlot);
                    marshaledSlot++;
                    slot++;
                } else {
                    slot += emitLoadParam(tryBlock, paramType, slot);
                }
            }
            tryBlock.invokevirtual(CD_MethodHandle, "invokeExact", buildInvokeExactDesc(nm));

            // Store return value before closing the arena (avoids having a live value on the
            // stack when we call arena.close()).
            if (returnType != NativeType.VOID) {
                emitStore(tryBlock, returnType, resultSlot);
            }

            tryBlock.aload(arenaSlot);
            tryBlock.invokeinterface(CD_Arena, "close", MTD_Arena_close);

            if (returnType != NativeType.VOID) {
                emitLoad(tryBlock, returnType, resultSlot);
            }
            emitTypedReturn(tryBlock, returnType);
        }, catchBuilder -> catchBuilder.catchingAll(catchBlock -> {
            // Stack on entry: [t]. Close arena, then wrap in AssertionError.
            catchBlock.aload(arenaSlot);
            catchBlock.invokeinterface(CD_Arena, "close", MTD_Arena_close);
            catchBlock.new_(CD_AssertionError);
            catchBlock.dup_x1();
            catchBlock.swap();
            catchBlock.invokespecial(CD_AssertionError, "<init>", MethodTypeDesc.of(CD_void, CD_Throwable));
            catchBlock.athrow();
        }));
    }

    /** Stores the top-of-stack value (of the given native type) into a local variable slot. */
    private static void emitStore(CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.istore(slot);
            case LONG -> cb.lstore(slot);
            case FLOAT -> cb.fstore(slot);
            case DOUBLE -> cb.dstore(slot);
            case ADDRESS, STRING -> cb.astore(slot);
            default -> throw new AssertionError("Cannot store type: " + type);
        }
    }

    /** Loads a value of the given native type from a local variable slot onto the stack. */
    private static void emitLoad(CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.iload(slot);
            case LONG -> cb.lload(slot);
            case FLOAT -> cb.fload(slot);
            case DOUBLE -> cb.dload(slot);
            case ADDRESS, STRING -> cb.aload(slot);
            default -> throw new AssertionError("Cannot load type: " + type);
        }
    }

    /**
     * Invokes the native function through its downcall MethodHandle. Each parameter is marshaled
     * to its FFM-compatible form before the call.
     */
    private static void emitInvokeExact(CodeBuilder cb, ClassDesc generatedDesc, MethodModel nm) {
        cb.getstatic(generatedDesc, nm.methodHandleFieldName(), CD_MethodHandle);

        int slot = 1; // slot 0 = this
        for (var paramType : nm.paramTypes()) {
            slot += emitLoadParam(cb, paramType, slot);
        }

        cb.invokevirtual(CD_MethodHandle, "invokeExact", buildInvokeExactDesc(nm));
    }

    /**
     * Loads a single parameter onto the stack. Returns the number of local-variable slots consumed
     * (2 for {@code long}/{@code double}, 1 for everything else).
     */
    private static int emitLoadParam(CodeBuilder cb, NativeType paramType, int slot) {
        switch (paramType) {
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
            default -> throw new AssertionError("Unhandled param type: " + paramType);
        }
    }

    private static void emitTypedReturn(CodeBuilder cb, NativeType returnType) {
        switch (returnType) {
            case VOID -> cb.return_();
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case STRING -> emitStringReturn(cb);
        }
    }

    /**
     * Marshals a {@code MemorySegment} returned by the native call into a Java {@code String},
     * returning {@code null} for a null pointer. Stack on entry: {@code [segment]}.
     */
    private static void emitStringReturn(CodeBuilder cb) {
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
        // Otherwise reinterpret the segment to a known size and read it as a UTF-8 string. We route
        // the read through MemorySegmentAdapter so the mrjar shim picks the right API for the runtime
        // JDK (MemorySegment.getString in JDK 22+, getUtf8String in JDK 21).
        cb.ldc(Long.MAX_VALUE);
        cb.invokeinterface(CD_MemorySegment, "reinterpret", MethodTypeDesc.of(CD_MemorySegment, CD_long));
        cb.ldc(0L);
        cb.invokestatic(CD_MemorySegmentAdapter, "getString", MTD_MemorySegmentAdapter_getString);
        cb.areturn();
    }

    // -------------------------------------------------------------------------
    // Descriptor helpers
    // -------------------------------------------------------------------------

    /** Builds the Java-facing method descriptor, using Java types for all parameters and the return type. */
    private static MethodTypeDesc buildJavaMethodDesc(MethodModel nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var paramType : nm.paramTypes()) {
            paramDescs.add(javaClassDesc(paramType));
        }
        return MethodTypeDesc.of(javaClassDesc(nm.returnType()), paramDescs);
    }

    /**
     * Builds the native-side descriptor for the downcall {@code MethodHandle.invokeExact} call.
     * {@code STRING} maps to {@code MemorySegment} on the wire — parameters are marshaled before
     * the call, return values are marshaled after.
     */
    private static MethodTypeDesc buildInvokeExactDesc(MethodModel nm) {
        List<ClassDesc> paramDescs = new ArrayList<>();
        for (var paramType : nm.paramTypes()) {
            paramDescs.add(nativeClassDesc(paramType));
        }
        return MethodTypeDesc.of(nativeClassDesc(nm.returnType()), paramDescs);
    }

    private static ClassDesc javaClassDesc(NativeType type) {
        return switch (type) {
            case VOID -> CD_void;
            case ADDRESS -> CD_MemorySegment;
            case STRING -> CD_String;
            default -> primitiveClassDesc(type);
        };
    }

    private static ClassDesc nativeClassDesc(NativeType type) {
        return switch (type) {
            case VOID -> CD_void;
            case ADDRESS, STRING -> CD_MemorySegment;
            default -> primitiveClassDesc(type);
        };
    }
}
